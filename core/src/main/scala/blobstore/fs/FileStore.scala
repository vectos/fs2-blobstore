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
package blobstore
package fs

import java.nio.file.{Paths, StandardOpenOption, Files => JFiles, Path => JPath}
import blobstore.url.{Authority, FsObject, Path, Url}
import cats.data.Validated
import cats.effect.std.Hotswap

import scala.jdk.CollectionConverters._
import cats.syntax.all._
import cats.effect.{Async, Resource}
import fs2.io.file.{FileHandle, Files, WriteCursor}
import fs2.{Pipe, Stream}

import scala.util.Try

class FileStore[F[_]](implicit F: Async[F]) extends PathStore[F, NioPath] {

  override def list[A](path: Path[A], recursive: Boolean = false): Stream[F, Path[NioPath]] = {
    val isDir  = Stream.eval(F.blocking(JFiles.isDirectory(path.nioPath)))
    val isFile = Stream.eval(F.blocking(JFiles.exists(path.nioPath)))

    val stream: Stream[F, (JPath, Boolean)] =
      Stream
        .eval(F.blocking(if (recursive) JFiles.walk(path.nioPath) else JFiles.list(path.nioPath)))
        .flatMap(x => Stream.fromBlockingIterator(x.iterator.asScala, 16))
        .flatMap { x =>
          val isDir = JFiles.isDirectory(x)
          if (recursive && isDir) {
            Stream.empty
          } else {
            Stream.emit(x -> isDir)
          }
        }

    val files = stream
      .evalMap {
        case (x, isDir) =>
          F.blocking(NioPath(
            x,
            Try(JFiles.size(x)).toOption,
            isDir,
            Try(JFiles.getLastModifiedTime(path.nioPath)).toOption.map(_.toInstant)
          ))
      }

    val file = Stream.eval {
      F.blocking {
        NioPath(
          path = Paths.get(path.show),
          size = Try(JFiles.size(path.nioPath)).toOption,
          isDir = false,
          lastModified = Try(JFiles.getLastModifiedTime(path.nioPath)).toOption.map(_.toInstant)
        )
      }
    }

    isDir.ifM(files, isFile.ifM(file, Stream.empty.covaryAll[F, NioPath])).map(p => Path.of(p.path.toString, p))
  }

  override def get[A](path: Path[A], chunkSize: Int): Stream[F, Byte] =
    Files[F].readAll(path.nioPath, chunkSize)

  override def put[A](path: Path[A], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] = {
    in =>
      val flags =
        if (overwrite) List(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
        else List(StandardOpenOption.CREATE_NEW)
      Stream.eval(createParentDir(path.plain)) >> Files[F].writeAll(
        path = path.nioPath,
        flags = flags
      ).apply(
        in
      )
  }

  override def move[A, B](src: Path[A], dst: Path[B]): F[Unit] =
    createParentDir(dst.plain) >> F.blocking(JFiles.move(src.nioPath, dst.nioPath)).void

  override def copy[A, B](src: Path[A], dst: Path[B]): F[Unit] =
    createParentDir(dst.plain) >> F.blocking(JFiles.copy(src.nioPath, dst.nioPath)).void

  override def remove[A](path: Path[A], recursive: Boolean = false): F[Unit] =
    if (recursive) {
      def recurse(path: JPath): Stream[F, JPath] =
        Files[F].directoryStream(path).flatMap {
          p =>
            Stream.eval(F.blocking(JFiles.isDirectory(p))).flatMap {
              case true =>
                recurse(p)
              case _ => Stream.empty
            } ++ Stream.emit(p)
        }
      recurse(path.nioPath).evalMap(p => F.blocking(JFiles.deleteIfExists(p))).compile.drain
    } else F.blocking(JFiles.deleteIfExists(path.nioPath)).void

  override def putRotate[A](computePath: F[Path[A]], limit: Long): Pipe[F, Byte, Unit] = { in =>
    val openNewFile: Resource[F, FileHandle[F]] =
      Resource
        .eval(computePath)
        .flatTap(p => Resource.eval(createParentDir(p.plain)))
        .flatMap { p =>
          Files[F].open(
            path = p.nioPath,
            flags = StandardOpenOption.CREATE :: StandardOpenOption.WRITE :: StandardOpenOption.TRUNCATE_EXISTING :: Nil
          )
        }

    def newCursor(file: FileHandle[F]): F[WriteCursor[F]] =
      Files[F].writeCursorFromFileHandle(file, append = false)

    Stream
      .resource(Hotswap(openNewFile))
      .flatMap {
        case (hotswap, fileHandle) =>
          Stream.eval(newCursor(fileHandle)).flatMap { cursor =>
            goRotate(limit, 0L, in, cursor, hotswap, openNewFile)(
              c => chunk => c.writePull(chunk),
              fh => Stream.eval(newCursor(fh))
            ).stream
          }
      }
  }

  private def createParentDir(p: Path.Plain): F[Unit] =
    F.blocking(JFiles.createDirectories(p.nioPath.getParent))
      .handleErrorWith(e => new Exception(s"failed to create dir: $p", e).raiseError).void

  override def stat[A](path: Path[A]): F[Option[Path[NioPath]]] =
    F.blocking {
      val p = path.nioPath

      if (!JFiles.exists(p)) None
      else
        path.as(NioPath(
          p,
          Try(JFiles.size(p)).toOption,
          Try(JFiles.isDirectory(p)).toOption.getOrElse(isDir(path)),
          Try(JFiles.getLastModifiedTime(path.nioPath)).toOption.map(_.toInstant)
        )).some
    }

  // The local file system can't have file names ending with slash
  private def isDir[A](path: Path[A]): Boolean = path.show.endsWith("/")

  /** Lifts this FileStore to a Store accepting URLs with authority `A` and exposing blobs of type `B`. You must provide
    * a mapping from this Store's BlobType to B, and you may provide a function `g` for controlling input paths to this store.
    *
    * Input URLs to the returned store are validated against this Store's authority before the path is extracted and passed
    * to this store.
    */
  override def liftTo[A <: Authority](g: Url[A] => Validated[Throwable, Path.Plain]): Store[F, A, NioPath] =
    new Store.DelegatingStore[F, A, NioPath](Right(this), g)

  def transferTo[A <: Authority, B, P](dstStore: Store[F, A, B], srcPath: Path[P], dstUrl: Url[A])(implicit
  ev: B <:< FsObject): F[Int] =
    defaultTransferTo(this, dstStore, srcPath, dstUrl)

  override def getContents[A](path: Path[A], chunkSize: Int): F[String] =
    get(path, chunkSize).through(fs2.text.utf8Decode).compile.string
}

object FileStore {
  def apply[F[_]: Async]: FileStore[F] = new FileStore
}
