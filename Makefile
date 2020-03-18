init:
	@cp .hooks/* .git/hooks
	@chmod +x .git/hooks
	@echo ""
	@echo "Set Git hooks up successfully. You're now ready to code!"
	@echo ""
.PHONY: init