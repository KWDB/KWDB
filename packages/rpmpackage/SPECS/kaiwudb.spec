Name:        %(echo ${KAIWUDB_PKG_NAME})
Version:     %(echo ${KAIWUDB_VERSION})
Release:     %{_vendor}
Summary:     KaiwuDB  bin and lib
# MIT etc
License:     NONE
# define the location of the rpmbuild directory
BuildRoot: %{_topdir}/BUILDROOT
URL:         https://kaiwudb.com/
#Source: ZDP
BuildRequires: make 
%description
KaiwuDB is an AIoT database product designed for industrial IoT, digital energy, connected vehicles, and smart industry scenarios. Boasting patented "in-situ computing" technology, it offers million-level writes per second, millisecond-level data reading capabilities, and satisfies the demands for massive, high-concurrency time-series data writing, as well as quick and complex querying. Additionally, it features cluster deployment, high availability, low cost, and easy maintenance.
This package contains the infrastructure needed to setup system databases.

%package server
Summary: KaiwuDB server binaries and libraries
Group: System Environment/Libraries
Requires: server-deps
Requires: %(echo ${KAIWUDB_PKG_NAME})-libcommon = %{version}
Suggests: geos
Autoprov: no
Autoreq: no
%description server
KaiwuDB server binaries and libraries.
KaiwuDB is a distributed multi-model database for AIoT, offering top-notch performance, reliability, and cost-effectiveness. It integrates various computing engines like adaptive time series, transaction processing, and machine learning to manage diverse data models efficiently. With advanced capabilities in distributed storage, efficient compression, and fast read/write operations, KaiwuDB is ideal for applications in Industrial IoT, Digital Energy, Vehicle-to-Everything, and Smart Industries. This package provides the essential infrastructure for setting up KaiwuDB.
This package contains the infrastructure needed to setup system databases.

%package libcommon
Summary: KaiwuDB common library
Group: System Environment/Libraries
Requires: common-deps
Autoprov: no
Autoreq: no
%description libcommon
KaiwuDB common library.
KaiwuDB is a distributed multi-model database for AIoT, offering top-notch performance, reliability, and cost-effectiveness. It integrates various computing engines like adaptive time series, transaction processing, and machine learning to manage diverse data models efficiently. With advanced capabilities in distributed storage, efficient compression, and fast read/write operations, KaiwuDB is ideal for applications in Industrial IoT, Digital Energy, Vehicle-to-Everything, and Smart Industries. This package provides the essential infrastructure for setting up KaiwuDB.
This package includes the common library.

#%prep preprocessing
%prep
%define _KaiwudbSourceDir %(echo $GOPATH)/src/gitee.com/kwbasedb
%define _topdir %{_KaiwudbSourceDir}/packages/rpmpackage

%define _KaiwudbLinkBinDir     /usr/bin
%define _KaiwudbLinkLibDir     /usr/lib
%define _prekwinstalldir      /usr/local
%define _KaiwudbPreBinInstall %{_prekwinstalldir}/kaiwudb/bin
%define _KaiwudbPreLibInstall %{_prekwinstalldir}/kaiwudb/lib
%define _Kaiwudbdocdir /usr/share/doc/kaiwudb
%build
# whether the build directory is exists, if not, building kwbase
if [ ! -d "%{_KaiwudbSourceDir}/build" ]; then

[ "%{_buildrootdir}" != "/" ] && %{__rm} -rf %{_buildrootdir}

[ "%{_sourcedir}" != "/" ] && %{__rm} -rf %{_sourcedir}
mkdir -p %{_KaiwudbSourceDir}/build
cd %{_KaiwudbSourceDir}
.CI/4_build_kwbase.sh
else
echo "KaiwuDB build is existed."
fi
# installing the binaries to virtual directory
%install
#%make_install
mkdir -p %{buildroot}%{_KaiwudbLinkBinDir}
mkdir -p %{buildroot}%{_KaiwudbLinkLibDir}

mkdir -p %{buildroot}%{_KaiwudbPreBinInstall}
mkdir -p %{buildroot}%{_KaiwudbPreLibInstall}
mkdir -p %{buildroot}%{_Kaiwudbdocdir}

#server
cp -rf  %{_KaiwudbSourceDir}/install/bin/kwbase %{buildroot}%{_KaiwudbPreBinInstall}
cp -rf  %{_KaiwudbSourceDir}/install/lib/libkwdbts2.so %{buildroot}%{_KaiwudbPreLibInstall}
cp -rf  %{_KaiwudbSourceDir}/NOTICE.txt %{buildroot}%{_Kaiwudbdocdir}/

#kaiwudbcommon
cp -rf  %{_KaiwudbSourceDir}/install/lib/libcommon.so %{buildroot}%{_KaiwudbPreLibInstall}
%clean
[ "%{_buildrootdir}" != "/" ] && %{__rm} -rf %{_buildrootdir}
[ "%{_sourcedir}" != "/" ] && %{__rm} -rf %{_sourcedir}
[ "%{_builddir}" != "/" ] && %{__rm} -rf %{_builddir}
[ "%{_srcrpmdir}" != "/" ] && %{__rm} -rf %{_srcrpmdir}

#rm -rf %{_KaiwudbSourceDir}/build
# define which binaries are installed
%files server
%defattr(-,root,root)
%{_KaiwudbPreBinInstall}/kwbase
%{_KaiwudbPreLibInstall}/libkwdbts2.so
%license %{_Kaiwudbdocdir}/NOTICE.txt

%files libcommon
%defattr(-,root,root)
%{_KaiwudbPreLibInstall}/libcommon.so

#%pre: pre-install
%pre server

# Execute the commands with sudo perimission
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

# Stop the service if running
pid=$(ps -ef | grep "kwbase" | grep -v "grep" | awk '{print $2}')
if [ -n "$pid" ]; then
    %{_KaiwudbPreBinInstall}/kwdbAdmin stop -d
fi

# if config, library and binary already softlink, remove it
#cfg_install_dir="/usr/share/kwdb"
#if [ -f "%{_KaiwudbPreConfInstall}/kaiwudb.cfg" ]; then
#    ${csudo}rm -f %{_KaiwudbPreConfInstall}/kaiwudb.cfg
#fi

# Remove bin and library
${csudo}rm -f %{_KaiwudbLinkBinDir}/kwbase
${csudo}rm -f %{_KaiwudbLinkLibDir}/libkwdbts2.so

#%post: post-install
%post server
# Execute the commands with sudo perimission
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

# Remove links
${csudo}rm -f %{_KaiwudbLinkBinDir}/kwbase
${csudo}rm -f %{_KaiwudbLinkLibDir}/libkwdbts2.so

# Make links
${csudo}ln -s %{_KaiwudbPreBinInstall}/kwbase %{_KaiwudbLinkBinDir}/kwbase
${csudo}ln -s %{_KaiwudbPreLibInstall}/libkwdbts2.so %{_KaiwudbLinkLibDir}/libkwdbts2.so

${csudo}echo "/usr/local/kaiwudb/lib" > /etc/ld.so.conf.d/kaiwudb.conf
${csudo}/sbin/ldconfig &> /dev/null
#%preun: pre-remove
%preun server
# Execute the commands with sudo perimission
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

# Stop the service if running
#pid=$(ps -ef | grep "kwbase" | grep -v "grep" | awk '{print $2}')
#if [ -n "$pid" ]; then
#    %{_KaiwudbPreBinInstall}/kwdbAdmin stop -d
#fi

# Remove links
${csudo}rm -f %{_KaiwudbLinkBinDir}/kwbase
${csudo}rm -f %{_KaiwudbLinkLibDir}/libkwdbts2.so
#%postun server: post-remove
%postun server

%pre libcommon
# Execute the commands with sudo perimission
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

# Remove library
${csudo}rm -rf %{_KaiwudbPreLibInstall}/libcommon.so

%post libcommon
# Execute the commands with sudo perimission
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

# Remove link
${csudo}rm -f %{_KaiwudbLinkLibDir}/libcommon.so

# Make link
${csudo}ln -s %{_KaiwudbPreLibInstall}/libcommon.so %{_KaiwudbLinkLibDir}/libcommon.so

${csudo}echo "/usr/local/kaiwudb/lib" > /etc/ld.so.conf.d/kaiwudb.conf
${csudo}/sbin/ldconfig &> /dev/null
%preun libcommon
# Execute the commands with sudo perimission
csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

# Remove link
${csudo}rm -f %{_KaiwudbLinkLibDir}/libcommon.so
%postun libcommon

%changelog
* Sun Feb 04 2024 KaiwuDB - 2.0.0
- Rebuilt for kaiwudb
* Tue Oct 24 2023 KaiwuDB - 1.2.2
- Rebuilt for kaiwudb
* Mon Sep 18 2023 KaiwuDB - 1.2.1
- Rebuilt for kaiwudb
* Tue Jun 06 2023 KaiwuDB - 1.2.0
- Rebuilt for kaiwudb
