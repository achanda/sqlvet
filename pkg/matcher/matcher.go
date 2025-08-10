package matcher

import (
	"go/types"
	"reflect"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"

	log "github.com/sirupsen/logrus"
)

type SqlFuncMatchRule struct {
	FuncName string `toml:"func_name"`
	// zero indexed
	QueryArgPos  int    `toml:"query_arg_pos"`
	QueryArgName string `toml:"query_arg_name"`
}

type SqlFuncMatcher struct {
	PkgPath string             `toml:"pkg_path"`
	Rules   []SqlFuncMatchRule `toml:"rules"`

	pkg *packages.Package
}

func (s *SqlFuncMatcher) SetGoPackage(p *packages.Package) { s.pkg = p }
func (s *SqlFuncMatcher) PackageImported() bool            { return s.pkg != nil }

func (s *SqlFuncMatcher) IterPackageExportedFuncs(cb func(*types.Func)) {
	scope := s.pkg.Types.Scope()
	for _, scopeName := range scope.Names() {
		obj := scope.Lookup(scopeName)
		if !obj.Exported() {
			continue
		}

		if fobj, ok := obj.(*types.Func); ok {
			cb(fobj)
		} else {
			// check for exported struct methods
			switch otype := obj.Type().(type) {
			case *types.Signature:
			case *types.Named:
				for i := 0; i < otype.NumMethods(); i++ {
					m := otype.Method(i)
					if !m.Exported() {
						continue
					}
					cb(m)
				}
			case *types.Basic:
			default:
				log.Debugf("Skipped pkg scope: %s (%s)", otype, reflect.TypeOf(otype))
			}
		}
	}
}

type MatchedSqlFunc struct {
	SSA         *ssa.Function
	QueryArgPos int
}

func (s *SqlFuncMatcher) MatchSqlFuncs(prog *ssa.Program) []MatchedSqlFunc {
	sqlfuncs := []MatchedSqlFunc{}
	s.IterPackageExportedFuncs(func(fobj *types.Func) {
		for _, rule := range s.Rules {
			if rule.FuncName != "" && fobj.Name() == rule.FuncName {
				sqlfuncs = append(sqlfuncs, MatchedSqlFunc{SSA: prog.FuncValue(fobj), QueryArgPos: rule.QueryArgPos})
				break
			}
			if rule.QueryArgName != "" {
				sigParams := fobj.Type().(*types.Signature).Params()
				if sigParams.Len()-1 < rule.QueryArgPos {
					continue
				}
				param := sigParams.At(rule.QueryArgPos)
				if param.Name() != rule.QueryArgName {
					continue
				}
				sqlfuncs = append(sqlfuncs, MatchedSqlFunc{SSA: prog.FuncValue(fobj), QueryArgPos: rule.QueryArgPos})
				break
			}
		}
	})
	return sqlfuncs
}
