package lightdb.doc

import lightdb.filter.NestedPathField

import scala.quoted.*

object NestedAccessMacros {
  inline def deriveAccess[A](path: String): A = ${ deriveAccessImpl[A]('path) }

  private def deriveAccessImpl[A: Type](pathExpr: Expr[String])(using q: Quotes): Expr[A] = {
    import q.reflect.*

    val accessTpe = TypeRepr.of[A]
    val accessSym = accessTpe.typeSymbol
    if !accessSym.flags.is(Flags.Trait) then {
      report.errorAndAbort(s"Nested access type must be a trait, found: ${accessSym.fullName}")
    }

    val npSym = TypeRepr.of[NestedPathField[?, ?]].typeSymbol

    val fields = accessSym.declaredMethods
      .filter(_.paramSymss.flatten.isEmpty)
      .filterNot(m => m.name == "$init$" || m.name == "toString" || m.name == "hashCode" || m.name == "equals")
      .map { m =>
        val out = accessTpe.memberType(m) match {
          case mt: MethodType => mt.resType
          case pt: PolyType => pt.resType
          case other => other
        }
        val widened = out.widenTermRefByName.dealias
        val valid = widened match {
          case AppliedType(t, List(_, _)) if t.typeSymbol == npSym => true
          case _ => false
        }
        if !valid then {
          report.errorAndAbort(
            s"Nested accessor '${m.name}' must be typed as NP[...] / NestedPathField[Doc, ...], found: ${widened.show}"
          )
        }
        m.name
      }

    val namesExpr = Expr.ofList(fields.map(Expr(_)))
    val classExpr = Literal(ClassOfConstant(accessTpe)).asExprOf[Class[?]]
    '{ NestedAccessRuntime.create[A]($pathExpr, $classExpr, $namesExpr) }
  }
}

