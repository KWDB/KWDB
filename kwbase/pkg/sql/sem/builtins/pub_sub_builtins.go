//
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Shanghai Yunxi Technology Co, Ltd makes no representations or warranties about the suitability
// of the software, either express or implied, including but not limited to the implied warranties
// of merchantability, fitness for a particular purpose, or non-infringement. Shanghai Yunxi
// Technology Co, Ltd shall not be liable for any damages suffered by licensee as a result
// of using, modifying or distributing this software or its derivatives.
//

package builtins

import (
	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func initPubAndSubBuiltins() {
	for k, v := range pubAndSubBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		//Add a new builtin category: publication and subscription
		v.props.Category = categoryPubAndSub
		builtins[k] = v
	}
}

// pubAndSubBuiltins builtins contains built-in functions about publication and subscription.
var pubAndSubBuiltins = map[string]builtinDefinition{
	// pub_sub_realtime subscribes the realtime insert data.
	"pub_sub_realtime": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryPubAndSub,
			Class:            tree.GeneratorClass,
			DistsqlBlacklist: true,
		},
		makeGeneratorOverload(
			tree.ArgTypes{
				{"publication_name", types.String},
				{"param", types.Bytes},
			},
			cdcpb.PubSubFuncType,
			func(evalCtx *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
				name := string(tree.MustBeDString(args[0]))
				param := []byte(tree.MustBeDBytes(args[1]))
				rows, err := cdcpb.GetPublicationsHook().SubscriptRealtime(evalCtx, name, param)
				if err != nil {
					return nil, err
				}

				return rows, nil
			},
			"Subscribe realtime data",
		),
	),

	// pub_sub_history subscribes the history data.
	"pub_sub_history": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryPubAndSub,
			Class:            tree.GeneratorClass,
			DistsqlBlacklist: true,
		},
		makeGeneratorOverload(
			tree.ArgTypes{
				{"publication_name", types.String},
				{"param", types.Bytes},
			},
			cdcpb.PubSubFuncType,
			func(evalCtx *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
				name := string(tree.MustBeDString(args[0]))
				param := []byte(tree.MustBeDBytes(args[1]))
				rows, err := cdcpb.GetPublicationsHook().SubscriptHistory(evalCtx, name, param)
				if err != nil {
					return nil, err
				}

				return rows, nil
			},
			"Subscribe history data",
		),
	),

	// pub_sub_get_info fetches the specified publication metadata and returns it
	// to the corresponding subscription.
	"pub_sub_get_info": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryPubAndSub,
			DistsqlBlacklist: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"publication_name", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := string(tree.MustBeDString(args[0]))
				res, err := cdcpb.GetPublicationsHook().GetPublicationInfo(evalCtx, name)
				if err != nil {
					return nil, err
				}

				return tree.NewDBytes(tree.DBytes(res)), nil
			},
		}),
}
