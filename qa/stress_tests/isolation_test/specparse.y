%{
/*-------------------------------------------------------------------------
 *
 * specparse.y
 *	  bison grammar for the isolation test file format
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 * 
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *-------------------------------------------------------------------------
 */

/*
#include "postgres_fe.h"
*/

#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>
#include "isolationtester.h"


TestSpec		parseresult;			/* result of parsing is left here */

%}

%expect 0
%name-prefix="spec_yy"

%union
{
	char	   *str;
	int			integer;
	Session	   *session;
	Step	   *step;
	Permutation *permutation;
	PermutationStep *permutationstep;
	PermutationStepBlocker *blocker;
	struct
	{
		void  **elements;
		int		nelements;
	}			ptr_list;
}

%type <ptr_list> setup_list
%type <str>  opt_setup opt_teardown
%type <str> setup
%type <ptr_list> step_list session_list permutation_list opt_permutation_list
%type <ptr_list> permutation_step_list blocker_list
%type <session> session
%type <step> step
%type <permutation> permutation
%type <permutationstep> permutation_step
%type <blocker> blocker

%token <str> sqlblock identifier
%token <integer> INTEGER
%token NOTICES PERMUTATION SESSION SETUP STEP TEARDOWN TEST

%%

TestSpec:
			setup_list
			opt_teardown
			session_list
			opt_permutation_list
			{
				parseresult.setupsqls = (char **) $1.elements;
				parseresult.nsetupsqls = $1.nelements;
				parseresult.teardownsql = $2;
				parseresult.sessions = (Session **) $3.elements;
				parseresult.nsessions = $3.nelements;
				parseresult.permutations = (Permutation **) $4.elements;
				parseresult.npermutations = $4.nelements;
			}
		;

setup_list:
			/* EMPTY */
			{
				$$.elements = NULL;
				$$.nelements = 0;
			}
			| setup_list setup
			{
				$$.elements = realloc($1.elements,
										 ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
		;

opt_setup:
			/* EMPTY */			{ $$ = NULL; }
			| setup				{ $$ = $1; }
		;

setup:
			SETUP sqlblock		{ $$ = $2; }
		;

opt_teardown:
			/* EMPTY */			{ $$ = NULL; }
			| TEARDOWN sqlblock	{ $$ = $2; }
		;

session_list:
			session_list session
			{
				$$.elements = realloc($1.elements,
										 ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
			| session
			{
				$$.nelements = 1;
				$$.elements = malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;

session:
			SESSION identifier opt_setup step_list opt_teardown
			{
				$$ = malloc(sizeof(Session));
				$$->name = $2;
				$$->setupsql = $3;
				$$->steps = (Step **) $4.elements;
				$$->nsteps = $4.nelements;
				$$->teardownsql = $5;
			}
		;

step_list:
			step_list step
			{
				$$.elements = realloc($1.elements,
										 ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
			| step
			{
				$$.nelements = 1;
				$$.elements = malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;


step:
			STEP identifier sqlblock
			{
				$$ = malloc(sizeof(Step));
				$$->name = $2;
				$$->sql = $3;
				$$->session = -1; /* until filled */
				$$->used = false;
			}
		;


opt_permutation_list:
			permutation_list
			{
				$$ = $1;
			}
			| /* EMPTY */
			{
				$$.elements = NULL;
				$$.nelements = 0;
			}

permutation_list:
			permutation_list permutation
			{
				$$.elements = realloc($1.elements,
										 ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
			| permutation
			{
				$$.nelements = 1;
				$$.elements = malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;


permutation:
			PERMUTATION permutation_step_list
			{
				$$ = malloc(sizeof(Permutation));
				$$->nsteps = $2.nelements;
				$$->steps = (PermutationStep **) $2.elements;
			}
		;

permutation_step_list:
			permutation_step_list permutation_step
			{
				$$.elements = realloc($1.elements,
										 ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $2;
				$$.nelements = $1.nelements + 1;
			}
			| permutation_step
			{
				$$.nelements = 1;
				$$.elements = malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;

permutation_step:
			identifier
			{
				$$ = malloc(sizeof(PermutationStep));
				$$->name = $1;
				$$->blockers = NULL;
				$$->nblockers = 0;
				$$->step = NULL;
			}
			| identifier '(' blocker_list ')'
			{
				$$ = malloc(sizeof(PermutationStep));
				$$->name = $1;
				$$->blockers = (PermutationStepBlocker **) $3.elements;
				$$->nblockers = $3.nelements;
				$$->step = NULL;
			}
		;

blocker_list:
			blocker_list ',' blocker
			{
				$$.elements = realloc($1.elements,
										 ($1.nelements + 1) * sizeof(void *));
				$$.elements[$1.nelements] = $3;
				$$.nelements = $1.nelements + 1;
			}
			| blocker
			{
				$$.nelements = 1;
				$$.elements = malloc(sizeof(void *));
				$$.elements[0] = $1;
			}
		;

blocker:
			identifier
			{
				$$ = malloc(sizeof(PermutationStepBlocker));
				$$->stepname = $1;
				$$->blocktype = PSB_OTHER_STEP;
				$$->num_notices = -1;
				$$->step = NULL;
				$$->target_notices = -1;
			}
			| identifier NOTICES INTEGER
			{
				$$ = malloc(sizeof(PermutationStepBlocker));
				$$->stepname = $1;
				$$->blocktype = PSB_NUM_NOTICES;
				$$->num_notices = $3;
				$$->step = NULL;
				$$->target_notices = -1;
			}
			| '*'
			{
				$$ = malloc(sizeof(PermutationStepBlocker));
				$$->stepname = NULL;
				$$->blocktype = PSB_ONCE;
				$$->num_notices = -1;
				$$->step = NULL;
				$$->target_notices = -1;
			}
		;

%%
