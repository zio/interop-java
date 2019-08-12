package zio
package interop

import _root_.java.net.InetSocketAddress
import _root_.java.nio.ByteBuffer
import _root_.java.nio.channels.{ AsynchronousServerSocketChannel, AsynchronousSocketChannel }
import _root_.java.util.concurrent.{ CompletableFuture, CompletionStage, Future }

import org.specs2.concurrent.ExecutionEnv
import zio.Cause.{ die, fail }
import zio.interop.java._

class javaSpec(implicit ee: ExecutionEnv) extends TestRuntime {

  def is = s2"""
  `Task.fromFutureJava` must
    be lazy on the `Future` parameter                    $lazyOnParamRef
    catch exceptions thrown by lazy block                $catchBlockException
    return an `IO` that fails if `Future` fails 1        $propagateExceptionFromFuture1
    return an `IO` that fails if `Future` fails 2        $propagateExceptionFromFuture2
    return an `IO` that produces the value from `Future` $produceValueFromFuture
    handle null produced by the completed `Future`       $handleNullFromFuture
  `Task.fromCompletionStage` must
    be lazy on the `Future` parameter                    $lazyOnParamRefCs
    catch exceptions thrown by lazy block                $catchBlockExceptionCs
    return an `IO` that fails if `Future` fails 1        $propagateExceptionFromCs1
    return an `IO` that fails if `Future` fails 2        $propagateExceptionFromCs2
    return an `IO` that produces the value from `Future` $produceValueFromCs
    handle null produced by the completed `Future`       $handleNullFromCs
  `Task.toCompletableFuture` must
    produce always a successful `IO` of `Future`         $toCompletableFutureAlwaysSucceeds
    be polymorphic in error type                         $toCompletableFuturePoly
    return a `CompletableFuture` that fails if `IO` fails           $toCompletableFutureFailed
    return a `CompletableFuture` that produces the value from `IO`  $toCompletableFutureValue
  `Task.toCompletableFutureE` must
    convert error of type `E` to `Throwable`             $toCompletableFutureE
  `Fiber.fromCompletionStage` must
    be lazy on the `Future` parameter                    $lazyOnParamRefFiberCs
    catch exceptions thrown by lazy block                $catchBlockExceptionFiberCs
    return an `IO` that fails if `Future` fails 1        $propagateExceptionFromFutureFiberCs1
    return an `IO` that fails if `Future` fails 2        $propagateExceptionFromFutureFiberCs2
    return an `IO` that produces the value from `Future` $produceValueFromFutureFiberCs
  `Fiber.fromFutureJava` must
    be lazy on the `Future` parameter                    $lazyOnParamRefFiberFuture
    catch exceptions thrown by lazy block                $catchBlockExceptionFiberFuture
    return an `IO` that fails if `Future` fails 1        $propagateExceptionFromFutureFiberFuture1
    return an `IO` that fails if `Future` fails 2        $propagateExceptionFromFutureFiberFuture2
    return an `IO` that produces the value from `Future` $produceValueFromFutureFiberFuture
  `Task.withCompletionHandler` must
    write and read to and from AsynchronousSocketChannel $withCompletionHandlerSocketChannels
  """

  def lazyOnParamRef = {
    var evaluated         = false
    def ftr: Future[Unit] = CompletableFuture.supplyAsync(() => evaluated = true)
    ZIO.fromFutureJava(UIO.succeedLazy(ftr))
    evaluated must beFalse
  }

  def catchBlockException = {
    val ex                          = new Exception("no future for you!")
    val noFuture: UIO[Future[Unit]] = UIO.succeedLazy(throw ex)
    unsafeRunSync(ZIO.fromFutureJava(noFuture)) must_=== Exit.Failure(die(ex))
  }

  def propagateExceptionFromFuture1 = {
    val ex                         = new Exception("no value for you!")
    val noValue: UIO[Future[Unit]] = UIO.succeedLazy(CompletableFuture_.failedFuture(ex))
    unsafeRunSync(ZIO.fromFutureJava(noValue)) must_=== Exit.Failure(fail(ex))
  }

  def propagateExceptionFromFuture2 = {
    val ex                         = new Exception("no value for you!")
    val noValue: UIO[Future[Unit]] = UIO.succeedLazy(CompletableFuture.supplyAsync(() => throw ex))
    unsafeRunSync(ZIO.fromFutureJava(noValue)) must_=== Exit.Failure(fail(ex))
  }

  def produceValueFromFuture = {
    val someValue: UIO[Future[Int]] = UIO.succeedLazy(CompletableFuture.completedFuture(42))
    unsafeRun(ZIO.fromFutureJava(someValue)) must_=== 42
  }

  def handleNullFromFuture = {
    val someValue: UIO[Future[String]] = UIO.succeedLazy(CompletableFuture.completedFuture[String](null))
    unsafeRun(ZIO.fromFutureJava[String](someValue)) must_=== null
  }

  def lazyOnParamRefCs = {
    var evaluated                 = false
    def cs: CompletionStage[Unit] = CompletableFuture.supplyAsync(() => evaluated = true)
    ZIO.fromCompletionStage(UIO.succeedLazy(cs))
    evaluated must beFalse
  }

  def catchBlockExceptionCs = {
    val ex                                   = new Exception("no future for you!")
    val noFuture: UIO[CompletionStage[Unit]] = UIO.succeedLazy(throw ex)
    unsafeRunSync(ZIO.fromCompletionStage(noFuture)) must_=== Exit.Failure(die(ex))
  }

  def propagateExceptionFromCs1 = {
    val ex                                  = new Exception("no value for you!")
    val noValue: UIO[CompletionStage[Unit]] = UIO.succeedLazy(CompletableFuture_.failedFuture(ex))
    unsafeRunSync(ZIO.fromCompletionStage(noValue)) must_=== Exit.Failure(fail(ex))
  }

  def propagateExceptionFromCs2 = {
    val ex                                  = new Exception("no value for you!")
    val noValue: UIO[CompletionStage[Unit]] = UIO.succeedLazy(CompletableFuture.supplyAsync(() => throw ex))
    unsafeRunSync(ZIO.fromCompletionStage(noValue)) must_=== Exit.Failure(fail(ex))
  }

  def produceValueFromCs = {
    val someValue: UIO[CompletionStage[Int]] = UIO.succeedLazy(CompletableFuture.completedFuture(42))
    unsafeRun(ZIO.fromCompletionStage(someValue)) must_=== 42
  }

  def handleNullFromCs = {
    val someValue: UIO[CompletionStage[String]] = UIO.succeedLazy(CompletableFuture.completedFuture[String](null))
    unsafeRun(ZIO.fromCompletionStage[String](someValue)) must_=== null
  }

  def toCompletableFutureAlwaysSucceeds = {
    val failedIO = IO.fail[Throwable](new Exception("IOs also can fail"))
    unsafeRun(failedIO.toCompletableFuture) must beAnInstanceOf[CompletableFuture[Unit]]
  }

  def toCompletableFuturePoly = {
    val unitIO: Task[Unit]                          = Task.unit
    val polyIO: IO[String, CompletableFuture[Unit]] = unitIO.toCompletableFuture
    val _                                           = polyIO // avoid warning
    ok
  }

  def toCompletableFutureFailed = {
    val failedIO: Task[Unit] = IO.fail[Throwable](new Exception("IOs also can fail"))
    unsafeRun(failedIO.toCompletableFuture).get() must throwA[Exception](message = "IOs also can fail")
  }

  def toCompletableFutureValue = {
    val someIO = Task.succeed[Int](42)
    unsafeRun(someIO.toCompletableFuture).get() must beEqualTo(42)
  }

  def toCompletableFutureE = {
    val failedIO: IO[String, Unit] = IO.fail[String]("IOs also can fail")
    unsafeRun(failedIO.toCompletableFutureWith(new Exception(_))).get() must throwA[Exception](
      message = "IOs also can fail"
    )
  }

  def lazyOnParamRefFiberCs = {
    var evaluated                  = false
    def ftr: CompletionStage[Unit] = CompletableFuture.supplyAsync(() => evaluated = true)
    Fiber.fromCompletionStage(ftr)
    evaluated must beFalse
  }

  def catchBlockExceptionFiberCs = {
    val ex                              = new Exception("no future for you!")
    def noFuture: CompletionStage[Unit] = throw ex
    unsafeRunSync(Fiber.fromCompletionStage(noFuture).join) must_=== Exit.Failure(die(ex))
  }

  def propagateExceptionFromFutureFiberCs1 = {
    val ex                             = new Exception("no value for you!")
    def noValue: CompletionStage[Unit] = CompletableFuture_.failedFuture(ex)
    unsafeRunSync(Fiber.fromCompletionStage(noValue).join) must_=== Exit.Failure(fail(ex))
  }

  def propagateExceptionFromFutureFiberCs2 = {
    val ex                             = new Exception("no value for you!")
    def noValue: CompletionStage[Unit] = CompletableFuture.supplyAsync(() => throw ex)
    unsafeRunSync(Fiber.fromCompletionStage(noValue).join) must_=== Exit.Failure(fail(ex))
  }

  def produceValueFromFutureFiberCs = {
    def someValue: CompletionStage[Int] = CompletableFuture.completedFuture(42)
    unsafeRun(Fiber.fromCompletionStage(someValue).join) must_=== 42
  }

  def lazyOnParamRefFiberFuture = {
    var evaluated         = false
    def ftr: Future[Unit] = CompletableFuture.supplyAsync(() => evaluated = true)
    Fiber.fromFutureJava(ftr)
    evaluated must beFalse
  }

  def catchBlockExceptionFiberFuture = {
    val ex                     = new Exception("no future for you!")
    def noFuture: Future[Unit] = throw ex
    unsafeRunSync(Fiber.fromFutureJava(noFuture).join) must_=== Exit.Failure(die(ex))
  }

  def propagateExceptionFromFutureFiberFuture1 = {
    val ex                    = new Exception("no value for you!")
    def noValue: Future[Unit] = CompletableFuture_.failedFuture(ex)
    unsafeRunSync(Fiber.fromFutureJava(noValue).join) must_=== Exit.Failure(fail(ex))
  }

  def propagateExceptionFromFutureFiberFuture2 = {
    val ex                    = new Exception("no value for you!")
    def noValue: Future[Unit] = CompletableFuture.supplyAsync(() => throw ex)
    unsafeRunSync(Fiber.fromFutureJava(noValue).join) must_=== Exit.Failure(fail(ex))
  }

  def produceValueFromFutureFiberFuture = {
    def someValue: Future[Int] = CompletableFuture.completedFuture(42)
    unsafeRun(Fiber.fromFutureJava(someValue).join) must_=== 42
  }

  def withCompletionHandlerSocketChannels = {
    val list: List[Byte] = List(13)
    val address          = new InetSocketAddress(54321)
    val server           = AsynchronousServerSocketChannel.open().bind(address)
    val client           = AsynchronousSocketChannel.open()

    val taskServer = for {
      c <- ZIO.withCompletionHandler[AsynchronousSocketChannel](server.accept((), _))
      w <- ZIO.withCompletionHandler[Integer](c.write(ByteBuffer.wrap(list.toArray), (), _))
    } yield w

    val taskClient = for {
      _      <- ZIO.withCompletionHandler[Void](client.connect(address, (), _))
      buffer = ByteBuffer.allocate(1)
      r      <- ZIO.withCompletionHandler[Integer](client.read(buffer, (), _))
    } yield (r, buffer.array.toList)

    val task = for {
      fiberServer  <- taskServer.fork
      fiberClient  <- taskClient.fork
      resultServer <- fiberServer.join
      resultClient <- fiberClient.join
    } yield (resultServer, resultClient)

    val actual = unsafeRun(task)
    actual must_=== ((1, (1, list)))
  }
}
