package com.learn.data.expression

import scala.reflect.runtime.universe.{Quasiquote, runtimeMirror}
import scala.tools.reflect.ToolBox

object ScalaExpressionHelper {

  private lazy val mirror = runtimeMirror(getClass.getClassLoader)
  private lazy val tb = ToolBox(mirror).mkToolBox()

  /**
    * Convert the function string to Scala Function object.
    * @param function, function in string
    * @return, Scala Function object
    */
  def eval[A, B](function: String): A => B = {
    val functionWrapper = s"object FunctionObject { $function }"
    val functionSymbol = tb.define(tb.parse(functionWrapper).asInstanceOf[tb.u.ImplDef])
    tb.eval(q"$functionSymbol.function _").asInstanceOf[A => B]
  }

}
