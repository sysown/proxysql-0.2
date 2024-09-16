#ifdef PROXYSQLCLICKHOUSE
#include "btree_map.h"
#include "proxysql.h"
#include "cpp.h"
#include "proxysql_atomic.h"
#ifndef SPOOKYV2
#include "SpookyV2.h"
#define SPOOKYV2
#endif

ClickHouse_Authentication::ClickHouse_Authentication() {
#ifdef DEBUG
	if (glovars.has_debug==false) {
#else
	if (glovars.has_debug==true) {
#endif /* DEBUG */
		perror("Incompatible debugging version");
		exit(EXIT_FAILURE);
	}
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_init(&creds_backends.lock, NULL);
	pthread_rwlock_init(&creds_frontends.lock, NULL);
#else
	spinlock_rwlock_init(&creds_backends.lock);
	spinlock_rwlock_init(&creds_frontends.lock);
#endif
	creds_backends.cred_array = new PtrArray();
	creds_frontends.cred_array = new PtrArray();
};

ClickHouse_Authentication::~ClickHouse_Authentication() {
	reset();
	delete creds_backends.cred_array;
	delete creds_frontends.cred_array;
};

void ClickHouse_Authentication::print_version() {
		fprintf(stderr,"Standard ProxySQL ClickHouse Authentication rev. %s -- %s -- %s\n", PROXYSQL_CLICKHOUSE_AUTHENTICATION_VERSION, __FILE__, __TIMESTAMP__);
	};

void ClickHouse_Authentication::set_all_inactive(enum cred_username_type usertype) {
	ch_creds_group_t &cg=(usertype==USERNAME_BACKEND ? creds_backends : creds_frontends);
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_wrlock(&cg.lock);
#else
	spin_wrlock(&cg.lock);
#endif
	unsigned int i;
	for (i=0; i<cg.cred_array->len; i++) {
		ch_account_details_t *ado=(ch_account_details_t *)cg.cred_array->index(i);
		ado->__active=false;
	}
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_unlock(&cg.lock);
#else
	spin_wrunlock(&cg.lock);
#endif
}

void ClickHouse_Authentication::remove_inactives(enum cred_username_type usertype) {
	ch_creds_group_t &cg=(usertype==USERNAME_BACKEND ? creds_backends : creds_frontends);
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_wrlock(&cg.lock);
#else
	spin_wrlock(&cg.lock);
#endif
	unsigned int i;
__loop_remove_inactives:
	for (i=0; i<cg.cred_array->len; i++) {
		ch_account_details_t *ado=(ch_account_details_t *)cg.cred_array->index(i);
		if (ado->__active==false) {
			del(ado->username,usertype,false);
			goto __loop_remove_inactives; // we aren't sure how the underlying structure changes, so we jump back to 0
		}
	}
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_unlock(&cg.lock);
#else
	spin_wrunlock(&cg.lock);
#endif
}

bool ClickHouse_Authentication::add(char * username, char * password, enum cred_username_type usertype, bool use_ssl, int default_hostgroup, char *default_schema, bool schema_locked, bool transaction_persistent, bool fast_forward, int max_connections) {
	uint64_t hash1, hash2;
	SpookyHash myhash;
	myhash.Init(1,2);
	myhash.Update(username,strlen(username));
	myhash.Final(&hash1,&hash2);

	ch_creds_group_t &cg=(usertype==USERNAME_BACKEND ? creds_backends : creds_frontends);
	
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_wrlock(&cg.lock);
#else
	spin_wrlock(&cg.lock);
#endif
	std::map<uint64_t, ch_account_details_t *>::iterator lookup;
	lookup = cg.bt_map.find(hash1);
	// few changes will follow, due to issue #802
	ch_account_details_t *ad=NULL;
	bool new_ad=false;
	if (lookup != cg.bt_map.end()) {
		ad=lookup->second;
		if (strcmp(ad->password,password)) {
			free(ad->password);
			ad->password=strdup(password);
			if (ad->sha1_pass) {
				free(ad->sha1_pass);
				ad->sha1_pass=NULL;
			}
		}
		if (strcmp(ad->default_schema,default_schema)) {
			free(ad->default_schema);
			ad->default_schema=strdup(default_schema);
		}
  } else {
		ad=(ch_account_details_t *)malloc(sizeof(ch_account_details_t));
		ad->username=strdup(username);
		ad->default_schema=strdup(default_schema);
		ad->password=strdup(password);
		new_ad=true;
		ad->sha1_pass=NULL;
		ad->num_connections_used=0;
	}

	ad->use_ssl=use_ssl;
	ad->default_hostgroup=default_hostgroup;
	ad->schema_locked=schema_locked;
	ad->transaction_persistent=transaction_persistent;
	ad->fast_forward=fast_forward;
	ad->max_connections=max_connections;
	ad->__active=true;
	if (new_ad) {
		cg.bt_map.insert(std::make_pair(hash1,ad));
		cg.cred_array->add(ad);
	}
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_unlock(&cg.lock);
#else
	spin_wrunlock(&cg.lock);
#endif
	return true;
};

int ClickHouse_Authentication::dump_all_users(ch_account_details_t ***ads, bool _complete) {
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_rdlock(&creds_frontends.lock);
	pthread_rwlock_rdlock(&creds_backends.lock);
#else
	spin_rdlock(&creds_frontends.lock);
	spin_rdlock(&creds_backends.lock);
#endif
	int total_size;
	int idx_=0;
	unsigned i=0;
	ch_account_details_t **_ads;
	total_size=creds_frontends.cred_array->len;
	if (_complete) {
		total_size+=creds_backends.cred_array->len;
	}
	if (!total_size) goto __exit_dump_all_users;
	_ads=(ch_account_details_t **)malloc(sizeof(ch_account_details_t *)*total_size);
	for (i=0; i<creds_frontends.cred_array->len; i++) {
		ch_account_details_t *ad=(ch_account_details_t *)malloc(sizeof(ch_account_details_t));
		ch_account_details_t *ado=(ch_account_details_t *)creds_frontends.cred_array->index(i);
		ad->username=strdup(ado->username);
		ad->max_connections=ado->max_connections;
		ad->default_hostgroup=ado->default_hostgroup;
		if (_complete==false) {
			ad->password=NULL;
			ad->default_schema=NULL;
			ad->num_connections_used=ado->num_connections_used;
		} else {
			ad->num_connections_used=ado->num_connections_used;
			ad->password=strdup(ado->password);
			ad->default_schema=strdup(ado->default_schema);
			ad->sha1_pass=NULL;
			ad->use_ssl=ado->use_ssl;
			ad->default_schema=strdup(ado->default_schema);
			ad->schema_locked=ado->schema_locked;
			ad->transaction_persistent=ado->transaction_persistent;
			ad->fast_forward=ado->fast_forward;
			ad->__frontend=1;
			ad->__backend=0;
		}
		_ads[idx_]=ad;
		idx_++;
	}
	if (_complete==true) {
	for (i=0; i<creds_backends.cred_array->len; i++) {
		ch_account_details_t *ad=(ch_account_details_t *)malloc(sizeof(ch_account_details_t));
		ch_account_details_t *ado=(ch_account_details_t *)creds_backends.cred_array->index(i);
		ad->num_connections_used=0;
		ad->username=strdup(ado->username);
		ad->password=strdup(ado->password);
		ad->sha1_pass=NULL;
		ad->use_ssl=ado->use_ssl;
		ad->default_hostgroup=ado->default_hostgroup;
		ad->default_schema=strdup(ado->default_schema);
		ad->schema_locked=ado->schema_locked;
		ad->transaction_persistent=ado->transaction_persistent;
		ad->fast_forward=ado->fast_forward;
		ad->max_connections=ado->max_connections;
		ad->__frontend=0;
		ad->__backend=1;
		_ads[idx_]=ad;
		idx_++;
	}
	}
	*ads=_ads;
__exit_dump_all_users:
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_unlock(&creds_frontends.lock);
	pthread_rwlock_unlock(&creds_backends.lock);
#else
	spin_rdunlock(&creds_frontends.lock);
	spin_rdunlock(&creds_backends.lock);
#endif
	return total_size;
}


int ClickHouse_Authentication::increase_frontend_user_connections(char *username, int *mc) {
	uint64_t hash1, hash2;
	SpookyHash *myhash=new SpookyHash();
	myhash->Init(1,2);
	myhash->Update(username,strlen(username));
	myhash->Final(&hash1,&hash2);
	delete myhash;
	ch_creds_group_t &cg=creds_frontends;
	int ret=0;
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_wrlock(&cg.lock);
#else
	spin_wrlock(&cg.lock);
#endif
	std::map<uint64_t, ch_account_details_t *>::iterator it;
	it = cg.bt_map.find(hash1);
	if (it != cg.bt_map.end()) {
		ch_account_details_t *ad=it->second;
		if (ad->max_connections > ad->num_connections_used) {
			ret=ad->max_connections-ad->num_connections_used;
			ad->num_connections_used++;
		}
		if (mc) {
			*mc=ad->max_connections;
		}
	}
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_unlock(&cg.lock);
#else
	spin_wrunlock(&cg.lock);
#endif
	return ret;
}

void ClickHouse_Authentication::decrease_frontend_user_connections(char *username) {
	uint64_t hash1, hash2;
	SpookyHash *myhash=new SpookyHash();
	myhash->Init(1,2);
	myhash->Update(username,strlen(username));
	myhash->Final(&hash1,&hash2);
	delete myhash;
	ch_creds_group_t &cg=creds_frontends;
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_wrlock(&cg.lock);
#else
	spin_wrlock(&cg.lock);
#endif
	std::map<uint64_t, ch_account_details_t *>::iterator it;
	it = cg.bt_map.find(hash1);
	if (it != cg.bt_map.end()) {
		ch_account_details_t *ad=it->second;
		if (ad->num_connections_used > 0) {
			ad->num_connections_used--;
		}
	}
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_unlock(&cg.lock);
#else
	spin_wrunlock(&cg.lock);
#endif
}

bool ClickHouse_Authentication::del(char * username, enum cred_username_type usertype, bool set_lock) {
	bool ret=false;
	uint64_t hash1, hash2;
	SpookyHash *myhash=new SpookyHash();
	myhash->Init(1,2);
	myhash->Update(username,strlen(username));
	myhash->Final(&hash1,&hash2);
	delete myhash;

	ch_creds_group_t &cg=(usertype==USERNAME_BACKEND ? creds_backends : creds_frontends);

	if (set_lock)
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
		pthread_rwlock_wrlock(&cg.lock);
#else
		spin_wrlock(&cg.lock);
#endif
	std::map<uint64_t, ch_account_details_t *>::iterator lookup;
	lookup = cg.bt_map.find(hash1);
	if (lookup != cg.bt_map.end()) {
		ch_account_details_t *ad=lookup->second;
		cg.cred_array->remove_fast(ad);
		cg.bt_map.erase(lookup);
		free(ad->username);
		free(ad->password);
		if (ad->sha1_pass) { free(ad->sha1_pass); ad->sha1_pass=NULL; }
		free(ad->default_schema);
		free(ad);
		ret=true;
	}
	if (set_lock)
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
		pthread_rwlock_unlock(&cg.lock);
#else
		spin_wrunlock(&cg.lock);
#endif
	return ret;
};

/* unused?
bool ClickHouse_Authentication::set_SHA1(char * username, enum cred_username_type usertype, void *sha_pass) {
	bool ret=false;
	uint64_t hash1, hash2;
	SpookyHash *myhash=new SpookyHash();
	myhash->Init(1,2);
	myhash->Update(username,strlen(username));
	myhash->Final(&hash1,&hash2);
	delete myhash;

	ch_creds_group_t &cg=(usertype==USERNAME_BACKEND ? creds_backends : creds_frontends);

#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_wrlock(&cg.lock);
#else
	spin_wrlock(&cg.lock);
#endif
	std::map<uint64_t, ch_account_details_t *>::iterator lookup;
	lookup = cg.bt_map.find(hash1);
	if (lookup != cg.bt_map.end()) {
		ch_account_details_t *ad=lookup->second;
		if (ad->sha1_pass) { free(ad->sha1_pass); ad->sha1_pass=NULL; }
		if (sha_pass) {
			ad->sha1_pass=malloc(SHA_DIGEST_LENGTH);
			memcpy(ad->sha1_pass,sha_pass,SHA_DIGEST_LENGTH);
		}
		ret=true;
	}
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_unlock(&cg.lock);
#else
   spin_wrunlock(&cg.lock);
#endif
	return ret;
};
*/

/*
// REMOVING THIS FUNCTION BECAUSE IT IS USED ONLY FOR LDAP
// AND CLICKHOUSE MODULE DOESN'T SUPPORT LDAP.
// See also MySQL_Protocol::process_pkt_handshake_response()
bool ClickHouse_Authentication::exists(char * username) {
	bool ret = false;
	uint64_t hash1, hash2;
	SpookyHash myhash;
	myhash.Init(1,2);
	myhash.Update(username,strlen(username));
	myhash.Final(&hash1,&hash2);

	ch_creds_group_t &cg = creds_frontends ;
	pthread_rwlock_rdlock(&cg.lock);
	std::map<uint64_t, ch_account_details_t *>::iterator lookup;
	lookup = cg.bt_map.find(hash1);
	if (lookup != cg.bt_map.end()) {
		ret = true;
	}
	pthread_rwlock_unlock(&cg.lock);
	return ret;
}
*/

ch_account_details_t ClickHouse_Authentication::lookup(
	char* username, enum cred_username_type usertype, const ch_dup_account_details_t& dup_details
) {
	ch_account_details_t ret {};

	uint64_t hash1, hash2;
	SpookyHash myhash;
	myhash.Init(1,2);
	myhash.Update(username,strlen(username));
	myhash.Final(&hash1,&hash2);

	ch_creds_group_t &cg=(usertype==USERNAME_BACKEND ? creds_backends : creds_frontends);

#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_rdlock(&cg.lock);
#else
	spin_rdlock(&cg.lock);
#endif
	std::map<uint64_t, ch_account_details_t *>::iterator lookup = cg.bt_map.find(hash1);

	if (lookup != cg.bt_map.end()) {
		ch_account_details_t* ad = lookup->second;

		ret.password = strdup(ad->password);
		ret.use_ssl = ad->use_ssl;
		ret.default_hostgroup = ad->default_hostgroup;

		if (dup_details.default_schema) {
			ret.default_schema = l_strdup(ad->default_schema);
		}

		ret.schema_locked = ad->schema_locked;
		ret.transaction_persistent = ad->transaction_persistent;
		ret.fast_forward = ad->fast_forward;
		ret.max_connections = ad->max_connections;

		if (dup_details.sha1_pass) {
			if (ad->sha1_pass) {
				ret.sha1_pass = malloc(SHA_DIGEST_LENGTH);
				memcpy(ret.sha1_pass,ad->sha1_pass,SHA_DIGEST_LENGTH);
			}
		}
	}

#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_unlock(&cg.lock);
#else
	spin_rdunlock(&cg.lock);
#endif

	return ret;
}

bool ClickHouse_Authentication::_reset(enum cred_username_type usertype) {
	ch_creds_group_t &cg=(usertype==USERNAME_BACKEND ? creds_backends : creds_frontends);

#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_wrlock(&cg.lock);
#else
	spin_wrlock(&cg.lock);
#endif
	std::map<uint64_t, ch_account_details_t *>::iterator lookup;

	while (cg.bt_map.size()) {
		lookup = cg.bt_map.begin();
		if ( lookup != cg.bt_map.end() ) {
			ch_account_details_t *ad=lookup->second;
     	cg.bt_map.erase(lookup);
			free(ad->username);
			free(ad->password);
			if (ad->sha1_pass) { free(ad->sha1_pass); ad->sha1_pass=NULL; }
			free(ad->default_schema);
			free(ad);
		}
	}
	while (cg.cred_array->len) {
		cg.cred_array->remove_index_fast(0);
	}
#ifdef PROXYSQL_AUTH_PTHREAD_MUTEX
	pthread_rwlock_unlock(&cg.lock);
#else
	spin_wrunlock(&cg.lock);
#endif
	return true;
};

bool ClickHouse_Authentication::reset() {
	_reset(USERNAME_BACKEND);
	_reset(USERNAME_FRONTEND);
	return true;
}
#endif /* PROXYSQLCLICKHOUSE */
