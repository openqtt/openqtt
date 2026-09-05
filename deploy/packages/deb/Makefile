TOPDIR := /tmp/emqx
# Keep this short to avoid bloating beam files with long file path info
SRCDIR := $(TOPDIR)/$(PKG_VSN)
BUILT := $(SRCDIR)/BUILT

OPENQTT_NAME=$(subst -pkg,,$(OPENQTT_BUILD))

TAR_PKG_DIR ?= _build/$(OPENQTT_BUILD)/rel/openqtt
TAR_PKG     := $(OPENQTT_REL)/$(TAR_PKG_DIR)/openqtt-$(PKG_VSN).tar.gz
SOURCE_PKG  := $(OPENQTT_NAME)_$(PKG_VSN)_$(shell dpkg --print-architecture)
TARGET_PKG  := $(OPENQTT_NAME)-$(shell $(OPENQTT_REL)/pkg-vsn.sh $(OPENQTT_NAME) --long)

.PHONY: all
all: | $(BUILT)
	cp -r debian $(SRCDIR)/
	cp emqx.service $(SRCDIR)/debian/${OPENQTT_NAME}.service
	sed -i "s#<DATETIME>#$(shell date -u '+%a, %d %b %Y %T %z')#g" $(SRCDIR)/debian/changelog
	sed -i "s#<VERSION>#$(PKG_VSN)#g" $(SRCDIR)/debian/changelog
	sed -i "s/emqx-pkg/$(OPENQTT_NAME)-pkg/g" $(SRCDIR)/debian/rules; \
	sed -i "s debian/emqx debian/$(OPENQTT_NAME) g" $(SRCDIR)/debian/rules; \
	sed -i "s/Package: emqx/Package: $(OPENQTT_NAME)/1" $(SRCDIR)/debian/control; \
	cd $(SRCDIR) && dpkg-buildpackage -us -uc
	mkdir -p $(OPENQTT_REL)/_packages/$(OPENQTT_NAME)
	cp $(SRCDIR)/../$(SOURCE_PKG).deb $(OPENQTT_REL)/_packages/$(OPENQTT_NAME)/$(TARGET_PKG).deb
	sha256sum $(OPENQTT_REL)/_packages/$(OPENQTT_NAME)/$(TARGET_PKG).deb | head -c 64 > \
		$(OPENQTT_REL)/_packages/$(OPENQTT_NAME)/$(TARGET_PKG).deb.sha256


$(BUILT):
	mkdir -p $(TOPDIR) $(SRCDIR)
	tar zxf $(TAR_PKG) -C $(SRCDIR)

clean:
	rm -rf $(SRCDIR)
