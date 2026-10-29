#!/usr/bin/make -f
	
	
calc: ast/VExprParser.l ast/VExprParser.y
	flex -is -oast/VExprParser.lex.cpp ast/VExprParser.l
	bison -dv -oast/VExprParser.tab.cpp ast/VExprParser.y
	clear

ast: ../../bosh/ast.dot
	dot -Tpng ../../bosh/ast.dot -o ../../bosh/ast.dot.png
	xdg-open ../../bosh/ast.dot.png

resolve: ../../bosh/resolve.dot
	dot -Tpng ../../bosh/resolve.dot -o ../../bosh/resolve.dot.png
	xdg-open ../../bosh/resolve.dot.png

reduce: ../../bosh/reduce.dot
	dot -Tpng ../../bosh/reduce.dot -o ../../bosh/reduce.dot.png
	xdg-open ../../bosh/reduce.dot.png
	

vlang: codegen/vlang.y codegen/vlang.l
	flex -s -ocodegen/vlang.lex.cpp codegen/vlang.l
	bison -dv -ocodegen/vlang.tab.cpp codegen/vlang.y
	clear
	
bosh: bosh/bosh.y bosh/bosh.l
	flex -s -obosh/bosh.lex.cpp bosh/bosh.l
	bison -dv -obosh/bosh.tab.cpp bosh/bosh.y
	clear
	