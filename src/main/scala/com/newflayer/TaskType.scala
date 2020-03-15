package com.newflayer

sealed trait TaskType

object TaskType {
  case object SimpleDockerContainer extends TaskType
  case class BashScript(scriptUrl: String, arguments: List[String]) extends TaskType
}
