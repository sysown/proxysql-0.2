#!/bin/bash
set -eu

echo "==> Build environment:"
env

ARCH=$(rpm --eval '%{_arch}')
echo "==> '${ARCH}' architecture detected for package"

DIST=$(source /etc/os-release; echo ${ID%%[-._ ]*}${VERSION%%[-._ ]*})
echo "==> '${DIST}' distro detected for package"

echo -e "==> C compiler: ${CC} -> $(readlink -e $(type -p ${CC}))\n$(${CC} --version)"
echo -e "==> C++ compiler: ${CXX} -> $(readlink -e $(type -p ${CXX}))\n$(${CXX} --version)"
#echo -e "==> linker version:\n$ ${LD} -> $(readlink -e $(type -p ${LD}))\n$(${LD} --version)"

echo "==> Cleaning"
# Delete package if exists
rm -f /opt/proxysql/binaries/proxysql-${CURVER}-1-${PKG_RELEASE}.${ARCH}.rpm || true
# Cleanup relic directories from a previously failed build
rm -fr /root/.pki /root/rpmbuild/{BUILDROOT,RPMS,SRPMS,BUILD,SOURCES,tmp} /opt/proxysql/proxysql /opt/proxysql/proxysql-${CURVER} || true

# Clean and build dependancies and source
echo "==> Building"
git config --system --add safe.directory '/opt/proxysql'
cd /opt/proxysql
echo "==> ProxySQL '$(git describe --long --abbrev=7)'"
#export SOURCE_DATE_EPOCH=$(git show -s --format=%ct HEAD)
#echo "==> Setting SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}"
#find /opt/proxysql -not -path "/opt/proxysql/binaries/*" -exec touch -h --date=@${SOURCE_DATE_EPOCH} {} \;

if [[ -z ${PROXYSQL_BUILD_TYPE:-} ]] ; then
	deps_target="build_deps"
	build_target=""
else
	deps_target="build_deps_$PROXYSQL_BUILD_TYPE"
	build_target="$PROXYSQL_BUILD_TYPE"
fi
#${MAKE} cleanbuild
${MAKE} ${MAKEOPT} "${deps_target}"

if [[ -z ${build_target} ]] ; then
	${MAKE} ${MAKEOPT}
else
	${MAKE} ${MAKEOPT} "${build_target}"
fi
touch /opt/proxysql/src/proxysql

# Prepare package files and build RPM
echo "==> Packaging"
mkdir -p proxysql/usr/bin proxysql/etc
cp src/proxysql proxysql/usr/bin/
cp -a systemd proxysql/etc/
cp -a etc/proxysql.cnf proxysql/etc/
cp -a etc/logrotate.d proxysql/etc/
mkdir -p proxysql/usr/share/proxysql/tools
cp -a tools/proxysql_galera_checker.sh tools/proxysql_galera_writer.pl proxysql/usr/share/proxysql/tools
mv proxysql "proxysql-${CURVER}"
tar czvf "proxysql-${CURVER}.tar.gz" proxysql-${CURVER}
mkdir -p /root/rpmbuild/{RPMS,SRPMS,BUILD,SOURCES,SPECS,tmp}
chown -R root:root /root/rpmbuild/SPECS
mv "/opt/proxysql/proxysql-${CURVER}.tar.gz" /root/rpmbuild/SOURCES
# build package
cd /root/rpmbuild && rpmbuild -ba SPECS/proxysql.spec --define "version ${CURVER}"
cp "/root/rpmbuild/RPMS/${ARCH}/proxysql-${CURVER}-1.${ARCH}.rpm" "/opt/proxysql/binaries/proxysql-${CURVER}-1-${PKG_RELEASE}.${ARCH}.rpm"
# get SHA1 of the packaged executable
mkdir -p /opt/proxysql/pkgroot/tmp
pushd /opt/proxysql/pkgroot
rpm2cpio /root/rpmbuild/RPMS/${ARCH}/proxysql-${CURVER}-1.${ARCH}.rpm | cpio -iu --to-stdout ./usr/bin/proxysql > tmp/proxysql
sha1sum tmp/proxysql | sed 's|tmp/||' | tee tmp/proxysql.sha1
cp tmp/proxysql.sha1 ../binaries/proxysql-${CURVER}-1-${PKG_RELEASE}.${ARCH}.id-hash
popd
# cleanup
rm -fr /root/.pki /root/rpmbuild/{BUILDROOT,RPMS,SRPMS,BUILD,SOURCES,tmp} /opt/proxysql/proxysql /opt/proxysql/proxysql-${CURVER} ./pkgroot
