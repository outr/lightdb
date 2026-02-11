package lightdb.doc

import lightdb.filter.NestedPathField

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import scala.collection.mutable
import fabric.rw.{RW, stringRW}

object NestedAccessRuntime {
  def create[A](path: String, accessClass: Class[?], fieldNames: List[String]): A = {
    if !accessClass.isInterface then {
      throw new IllegalArgumentException(s"Nested access type must be a trait/interface, but found: ${accessClass.getName}")
    }
    val ctor = classOf[NestedPathField[?, ?]].getConstructor(classOf[String], classOf[RW[?]])
    val values = mutable.Map.empty[String, AnyRef]
    fieldNames.foreach { name =>
      values.put(name, ctor.newInstance(s"$path.$name", stringRW.asInstanceOf[RW[?]]).asInstanceOf[AnyRef])
    }
    val handler = new InvocationHandler {
      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = method.getName match {
        case "toString" if method.getParameterCount == 0 =>
          s"${accessClass.getSimpleName}(${fieldNames.mkString(", ")})"
        case "hashCode" if method.getParameterCount == 0 =>
          Int.box(System.identityHashCode(proxy))
        case "equals" if method.getParameterCount == 1 =>
          java.lang.Boolean.valueOf(proxy.asInstanceOf[AnyRef] eq args(0))
        case name =>
          values.getOrElseUpdate(name, ctor.newInstance(s"$path.$name", stringRW.asInstanceOf[RW[?]]).asInstanceOf[AnyRef]).asInstanceOf[AnyRef]
      }
    }
    Proxy
      .newProxyInstance(accessClass.getClassLoader, Array(accessClass), handler)
      .asInstanceOf[A]
  }
}

