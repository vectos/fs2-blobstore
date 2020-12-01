/*
Copyright 2018 LendUp Global, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

import blobstore.url.{Authority, FsObject, Path, Url}
import cats.effect.std.Hotswap
import cats.effect.{Concurrent, MonadCancelThrow, Resource}
import fs2.{Chunk, Pipe, Pull, RaiseThrowable, Stream}
import fs2.io.file.Files
import cats.implicits._

package object blobstore {

  protected[blobstore] def bufferToDisk[F[_]: MonadCancelThrow](
    chunkSize: Int
  )(implicit files: Files[F]): Pipe[F, Byte, (Long, Stream[F, Byte])] = { in =>
    Stream.resource(Files[F].tempFile(prefix = "bufferToDisk")).flatMap { p =>
      in.through(files.writeAll(p)).drain ++ Stream.emit((p.toFile.length, files.readAll(p, chunkSize)))
    }
  }

  private[blobstore] def putRotateBase[F[_]: Concurrent, T](
    limit: Long,
    openNewFile: Resource[F, T]
  )(consume: T => Chunk[Byte] => F[Unit]): Pipe[F, Byte, Unit] = { in =>
    Stream
      .resource(Hotswap(openNewFile))
      .flatMap {
        case (hotswap, newFile) =>
          goRotate(limit, 0L, in, newFile, hotswap, openNewFile)(
            consume = consumer => bytes => Pull.eval(consume(consumer)(bytes)).as(consumer),
            extract = Stream.emit
          ).stream
      }
  }

  private[blobstore] def goRotate[F[_]: RaiseThrowable, A, B](
    limit: Long,
    acc: Long,
    s: Stream[F, Byte],
    consumer: B,
    hotswap: Hotswap[F, A],
    resource: Resource[F, A]
  )(
    consume: B => Chunk[Byte] => Pull[F, Unit, B],
    extract: A => Stream[F, B]
  ): Pull[F, Unit, Unit] = {
    val toWrite = (limit - acc).min(Int.MaxValue.toLong).toInt
    s.pull.unconsLimit(toWrite).flatMap {
      case Some((hd, tl)) =>
        val newAcc = acc + hd.size
        consume(consumer)(hd).flatMap { consumer =>
          if (newAcc >= limit) {
            Pull
              .eval(hotswap.swap(resource))
              .flatMap(a => extract(a).pull.headOrError)
              .flatMap(nc => goRotate(limit, 0L, tl, nc, hotswap, resource)(consume, extract))
          } else {
            goRotate(limit, newAcc, tl, consumer, hotswap, resource)(consume, extract)
          }
        }
      case None => Pull.done
    }
  }

  private[blobstore] def defaultTransferTo[F[_]: Concurrent, A <: Authority, B, P, C](
    selfStore: PathStore[F, C],
    dstStore: Store[F, A, B],
    srcPath: Path[P],
    dstUrl: Url[A]
  )(implicit evB: B <:< FsObject, evC: C <:< FsObject): F[Int] =
    dstStore.stat(dstUrl).last.map(_.fold(dstUrl.path.show.endsWith("/"))(_.isDir)).flatMap { dstIsDir =>
      selfStore.list(srcPath.plain, recursive = false)
        .evalMap { p =>
          if (p.isDir) {
            defaultTransferTo(selfStore, dstStore, p, dstUrl `//` p.lastSegment)
          } else {
            val dp = if (dstIsDir) dstUrl / p.lastSegment else dstUrl
            selfStore.get(p, 4096).through(dstStore.put(dp)).compile.drain.as(1)
          }
        }
        .fold(0)(_ + _)
    }.compile.lastOrError

}
