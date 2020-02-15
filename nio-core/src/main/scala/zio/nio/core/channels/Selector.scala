package zio.nio.core.channels

import java.io.IOException
import java.nio.channels.{ ClosedSelectorException, Selector => JSelector }

import zio.{ IO, UIO }
import com.github.ghik.silencer.silent
import zio.duration.Duration
import zio.nio.core.channels.spi.SelectorProvider

import scala.collection.JavaConverters

class Selector(private[nio] val selector: JSelector) {
  final val isOpen: UIO[Boolean] = IO.effectTotal(selector.isOpen)

  final val provider: UIO[SelectorProvider] =
    IO.effectTotal(selector.provider()).map(new SelectorProvider(_))

  @silent
  final val keys: IO[ClosedSelectorException, Set[SelectionKey]] =
    IO.effect(selector.keys())
      .map { keys =>
        JavaConverters.asScalaSet(keys).toSet.map(new SelectionKey(_))
      }
      .refineToOrDie[ClosedSelectorException]

  @silent
  final val selectedKeys: IO[ClosedSelectorException, Set[SelectionKey]] =
    IO.effect(selector.selectedKeys())
      .map { keys =>
        JavaConverters.asScalaSet(keys).toSet.map(new SelectionKey(_))
      }
      .refineToOrDie[ClosedSelectorException]

  final def removeKey(key: SelectionKey): IO[ClosedSelectorException, Unit] =
    IO.effect(selector.selectedKeys().remove(key.selectionKey))
      .unit
      .refineToOrDie[ClosedSelectorException]

  /**
   * Can throw IOException and ClosedSelectorException.
   */
  final val selectNow: IO[Exception, Int] =
    IO.effect(selector.selectNow()).refineToOrDie[Exception]

  /**
   * Performs a blocking select operation.
   * 
   * **Note this will very often block**.
   * This is intended to be used when the effect is locked to an Executor
   * that is appropriate for this.
   *
   * Can throw IOException and ClosedSelectorException.
   */
  final def select(timeout: Duration): IO[Exception, Int] =
    IO.effect(selector.select(timeout.toMillis)).refineToOrDie[Exception]

  /**
   * Performs a blocking select operation.
   * 
   * **Note this will very often block**.
   * This is intended to be used when the effect is locked to an Executor
   * that is appropriate for this.
   * 
   * Can throw IOException and ClosedSelectorException.
   */
  final def select: IO[Exception, Int] =
    IO.effect(selector.select()).refineToOrDie[IOException]

  final val wakeup: IO[Nothing, Selector] =
    IO.effectTotal(selector.wakeup()).unit

  final val close: IO[IOException, Unit] =
    IO.effect(selector.close()).refineToOrDie[IOException]
}

object Selector {

  final val make: IO[IOException, Selector] =
    IO.effect(new Selector(JSelector.open())).refineToOrDie[IOException]
}
