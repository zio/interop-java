/*
 * Copyright 2017-2019 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.interop

import java.nio.channels.CompletionHandler
import java.util.concurrent.{ CompletableFuture, CompletionException, CompletionStage, Future }

import zio._
import zio.blocking.{ blocking, Blocking }

import scala.concurrent.ExecutionException
import scala.util.control.NonFatal

object javaconcurrent {

  def withCompletionHandler[T](op: CompletionHandler[T, Any] => Unit): Task[T] =
    Task.effectAsync[T] { k =>
      val handler = new CompletionHandler[T, Any] {
        def completed(result: T, u: Any): Unit =
          k(Task.succeed(result))

        def failed(t: Throwable, u: Any): Unit =
          t match {
            case NonFatal(e) => k(Task.fail(e))
            case _           => k(Task.die(t))
          }
      }

      try {
        op(handler)
      } catch {
        case NonFatal(e) => k(Task.fail(e))
      }
    }

  def fromCompletionStage[A](csUio: UIO[CompletionStage[A]]): Task[A] =
    csUio.flatMap { cs =>
      Task.effectAsync { cb =>
        val _ = cs.handle[Unit] { (v: A, t: Throwable) =>
          val io = t match {
            case null =>
              Task.succeed(v)
            case e: CompletionException =>
              Task.fail(e.getCause)
            case NonFatal(e) =>
              Task.fail(e)
            case _ =>
              Task.die(t)
          }
          cb(io)
        }
      }
    }

  /** WARNING: this uses the blocking Future#get, consider using `fromCompletionStage` */
  def fromFutureJava[A](futureUio: UIO[Future[A]]): Task[A] =
    futureUio.flatMap { future =>
      def unwrap[B](f: Future[B]): Task[B] =
        Task.flatten {
          Task.effect {
            try {
              val result = f.get()
              Task.succeed(result)
            } catch {
              case e: ExecutionException =>
                Task.fail(e.getCause)
              case _: InterruptedException =>
                Task.interrupt
            }
          }
        }

      for {
        isDone <- Task.effect(future.isDone)
        result <- if (isDone) {
                   unwrap(future)
                 } else {
                   blocking(unwrap(future)).provide(Blocking.Live)
                 }
      } yield result
    }

  implicit class CompletionStageJavaconcurrentOps[A](private val csUio: UIO[CompletionStage[A]]) extends AnyVal {
    def toZio: Task[A] = Task.fromCompletionStage(csUio)
  }

  implicit class FutureJavaconcurrentOps[A](private val futureUio: UIO[Future[A]]) extends AnyVal {

    /** WARNING: this uses the blocking Future#get, consider using `CompletionStage` */
    def toZio: Task[A] = Task.fromFutureJava(futureUio)
  }

  implicit class TaskObjJavaconcurrentOps(private val taskObj: Task.type) extends AnyVal {

    def withCompletionHandler[T](op: CompletionHandler[T, Any] => Unit): Task[T] =
      javaconcurrent.withCompletionHandler(op)

    def fromCompletionStage[A](csUio: UIO[CompletionStage[A]]): Task[A] = javaconcurrent.fromCompletionStage(csUio)

    /** WARNING: this uses the blocking Future#get, consider using `fromCompletionStage` */
    def fromFutureJava[A](futureUio: UIO[Future[A]]): Task[A] = javaconcurrent.fromFutureJava(futureUio)

  }

  implicit class ZioObjJavaconcurrentOps(private val taskObj: ZIO.type) extends AnyVal {

    def withCompletionHandler[T](op: CompletionHandler[T, Any] => Unit): Task[T] =
      javaconcurrent.withCompletionHandler(op)

    def fromCompletionStage[A](csUio: UIO[CompletionStage[A]]): Task[A] = javaconcurrent.fromCompletionStage(csUio)

    /** WARNING: this uses the blocking Future#get, consider using `fromCompletionStage` */
    def fromFutureJava[A](futureUio: UIO[Future[A]]): Task[A] = javaconcurrent.fromFutureJava(futureUio)

  }

  implicit class FiberObjOps(private val fiberObj: Fiber.type) extends AnyVal {

    def fromCompletionStage[A](thunk: => CompletionStage[A]): Fiber[Throwable, A] = {

      lazy val cf = thunk.toCompletableFuture

      new Fiber[Throwable, A] {

        override def await: UIO[Exit[Throwable, A]] = Task.fromCompletionStage(UIO.succeed(cf)).run

        override def poll: UIO[Option[Exit[Throwable, A]]] =
          UIO.effectSuspendTotal {
            if (cf.isDone) {
              Task
                .fromCompletionStage(UIO.succeed(cf))
                .fold(Exit.fail, Exit.succeed)
                .map(Some(_))
            } else {
              UIO.succeed(None)
            }
          }

        override def interrupt: UIO[Exit[Throwable, A]] = join.fold(Exit.fail, Exit.succeed)

        override def inheritFiberRefs: UIO[Unit] = UIO.unit
      }
    }

    def fromFutureJava[A](thunk: => Future[A]): Fiber[Throwable, A] = {

      lazy val ftr = thunk

      new Fiber[Throwable, A] {

        def await: UIO[Exit[Throwable, A]] =
          Task.fromFutureJava(UIO.effectTotal(ftr)).run

        def poll: UIO[Option[Exit[Throwable, A]]] =
          UIO.effectSuspendTotal {
            if (ftr.isDone) {
              Task
                .fromFutureJava(UIO.effectTotal(ftr))
                .fold(Exit.fail, Exit.succeed)
                .map(Some(_))
            } else {
              UIO.succeed(None)
            }
          }

        def interrupt: UIO[Exit[Throwable, A]] =
          join.fold(Exit.fail, Exit.succeed)

        def inheritFiberRefs: UIO[Unit] = UIO.unit
      }
    }
  }

  /**
   * CompletableFuture#failedFuture(Throwable) available only since Java 9
   */
  object CompletableFuture_ {
    def failedFuture[A](e: Throwable): CompletableFuture[A] = {
      val f = new CompletableFuture[A]
      f.completeExceptionally(e)
      f
    }
  }

  implicit class IOThrowableOps[A](private val io: Task[A]) extends AnyVal {
    def toCompletableFuture: UIO[CompletableFuture[A]] =
      io.fold(CompletableFuture_.failedFuture, CompletableFuture.completedFuture[A])
  }

  implicit class IOOps[E, A](private val io: IO[E, A]) extends AnyVal {
    def toCompletableFutureWith(f: E => Throwable): UIO[CompletableFuture[A]] =
      io.mapError(f).toCompletableFuture
  }

}
