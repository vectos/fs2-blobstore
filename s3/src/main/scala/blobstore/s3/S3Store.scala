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
package s3

import java.nio.ByteBuffer
import java.util.concurrent.{CompletableFuture, CompletionException}
import blobstore.url.{Authority, Path, Url}
import blobstore.Store.{BlobStore, DelegatingStore}
import cats.effect.{Async, Resource, Sync}
import cats.syntax.all._
import cats.MonadError
import cats.effect.std.{Hotswap, Queue}
import fs2.{Chunk, Pipe, Pull, Stream}
import fs2.interop.reactivestreams._
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer, SdkPublisher}
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model._

import scala.jdk.CollectionConverters._

/** @param s3 - S3 Async Client
  * @param objectAcl - optional default ACL to apply to all put, move and copy operations.
  * @param sseAlgorithm - optional default SSE Algorithm to apply to all put, move and copy operations.
  * @param defaultFullMetadata       – return full object metadata on [[list]], requires additional request per object.
  *                                  Metadata returned by default: size, lastModified, eTag, storageClass.
  *                                  This controls behaviour of [[list]] method from Store trait.
  *                                  Use [[listUnderlying]] to control on per-invocation basis.
  * @param defaultTrailingSlashFiles - test if folders returned by [[list]] are files with trailing slashes in their names.
  *                                  This controls behaviour of [[list]] method from Store trait.
  *                                  Use [[listUnderlying]] to control on per-invocation basis.
  * @param bufferSize - size of buffer for multipart uploading (used for large streams without size known in advance).
  *                     @see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
  */
class S3Store[F[_]](
  s3: S3AsyncClient,
  objectAcl: Option[ObjectCannedACL] = None,
  sseAlgorithm: Option[String] = None,
  defaultFullMetadata: Boolean = false,
  defaultTrailingSlashFiles: Boolean = false,
  bufferSize: Int = S3Store.multiUploadDefaultPartSize.toInt,
  queueSize: Int = 32
)(implicit F: Async[F])
  extends BlobStore[F, S3Blob] {
  require(
    bufferSize >= S3Store.multiUploadMinimumPartSize,
    s"Buffer size must be at least ${S3Store.multiUploadMinimumPartSize}"
  )

  override def list(url: Url[Authority.Bucket], recursive: Boolean = false): Stream[F, Path[S3Blob]] =
    listUnderlying(url, defaultFullMetadata, defaultTrailingSlashFiles, recursive)

  override def get(url: Url[Authority.Bucket], chunkSize: Int): Stream[F, Byte] = get(url, None)

  def get(url: Url[Authority.Bucket], meta: Option[S3MetaInfo]): Stream[F, Byte] = {
    val bucket = url.bucket.show
    val key    = url.path.relative.show

    val request: GetObjectRequest =
      meta.fold(GetObjectRequest.builder())(S3MetaInfo.getObjectRequest).bucket(bucket).key(key).build()
    val cf = new CompletableFuture[Stream[F, Byte]]()
    val transformer: AsyncResponseTransformer[GetObjectResponse, Stream[F, Byte]] =
      new AsyncResponseTransformer[GetObjectResponse, Stream[F, Byte]] {
        override def prepare(): CompletableFuture[Stream[F, Byte]] = cf
        override def onResponse(response: GetObjectResponse): Unit = ()
        override def onStream(publisher: SdkPublisher[ByteBuffer]): Unit = {
          cf.complete(publisher.toStream.flatMap(bb => Stream.chunk(Chunk.byteBuffer(bb))))
          ()
        }
        override def exceptionOccurred(error: Throwable): Unit = {
          cf.completeExceptionally(error)
          ()
        }
      }
    Stream.eval(Async[F].fromCompletableFuture(F.delay(s3.getObject[Stream[F, Byte]](request, transformer)))).flatten
  }

  def put(url: Url[Authority.Bucket], overwrite: Boolean = true, size: Option[Long] = None): Pipe[F, Byte, Unit] =
    put(url, overwrite, size, None)

  def put(
    url: Url[Authority.Bucket],
    overwrite: Boolean,
    size: Option[Long],
    meta: Option[S3MetaInfo]
  ): Pipe[F, Byte, Unit] =
    in => {
      val bucket = url.bucket.show
      val key    = url.path.relative.show

      val checkOverwrite =
        if (!overwrite) {
          Async[F].fromCompletableFuture(
            F.delay(s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build()))
          ).attempt
            .flatMap {
              case Left(e: CompletionException) =>
                Option(e.getCause) match {
                  case Some(_: NoSuchKeyException) => F.unit
                  case _                           => Option(e.getCause).getOrElse(e).raiseError[F, Unit]
                }
              case Left(e) => F.raiseError[Unit](e)
              case Right(_) =>
                F.raiseError[Unit](new IllegalArgumentException(s"File at path '$url' already exist."))
            }
        } else F.unit

      Stream.eval(checkOverwrite) ++
        size.fold(putUnknownSize(bucket, key, meta, in))(size => putKnownSize(bucket, key, meta, size, in))
    }

  override def move(src: Url[Authority.Bucket], dst: Url[Authority.Bucket]): F[Unit] =
    copy(src, dst) >> remove(src)

  override def copy(src: Url[Authority.Bucket], dst: Url[Authority.Bucket]): F[Unit] = {
    stat(dst).compile.last.flatMap(s => copy(src, dst, s.flatMap(_.representation.meta)))
  }

  def copy(src: Url[Authority.Bucket], dst: Url[Authority.Bucket], dstMeta: Option[S3MetaInfo]): F[Unit] = {
    val request = {
      val srcBucket = src.bucket.show
      val srcKey    = src.path.relative.show
      val dstBucket = dst.bucket.show
      val dstKey    = dst.path.relative.show

      val builder = CopyObjectRequest.builder()
      val withAcl = objectAcl.fold(builder)(builder.acl)
      val withSSE = sseAlgorithm.fold(withAcl)(withAcl.serverSideEncryption)
      dstMeta
        .fold(withSSE)(S3MetaInfo.copyObjectRequest(withSSE))
        .copySource(srcBucket ++ "/" ++ srcKey)
        .destinationBucket(dstBucket)
        .destinationKey(dstKey)
        .build()
    }
    Async[F].fromCompletableFuture(F.delay(s3.copyObject(request))).void
  }

  override def remove(url: Url[Authority.Bucket], recursive: Boolean = false): F[Unit] = {
    val bucket = url.bucket.show
    val key    = url.path.relative.show
    val req    = DeleteObjectRequest.builder().bucket(bucket).key(key).build()

    if (recursive) new StoreOps[F, Authority.Bucket, S3Blob](this).removeAll(url).void
    else Async[F].fromCompletableFuture(F.delay(s3.deleteObject(req))).void
  }

  override def putRotate(computePath: F[Url[Authority.Bucket]], limit: Long): Pipe[F, Byte, Unit] =
    if (limit <= bufferSize.toLong) {
      _.chunkN(limit.toInt)
        .flatMap(chunk => Stream.eval(computePath).flatMap(path => Stream.chunk(chunk).through(put(path))))
    } else {
      val newFile = for {
        path  <- Resource.eval(computePath)
        queue <- Resource.eval(Queue.bounded[F, Option[Chunk[Byte]]](queueSize))
        _ <- Resource.make(
          F.start(Stream.repeatEval(queue.take).unNoneTerminate.flatMap(Stream.chunk).through(put(path)).compile.drain)
        )(_.join.void)
        _ <- Resource.make(F.unit)(_ => queue.offer(None))
      } yield queue
      putRotateBase(limit, newFile)(queue => chunk => queue.offer(Some(chunk)))
    }

  def listUnderlying(
    url: Url[Authority.Bucket],
    fullMetadata: Boolean,
    expectTrailingSlashFiles: Boolean,
    recursive: Boolean
  ): Stream[F, Path[S3Blob]] = {
    val bucket = url.authority.show
    val key    = url.path.relative.show
    val request = {
      val b = ListObjectsV2Request
        .builder()
        .bucket(bucket.show)
        .prefix(if (key == "/") "" else key)
      val builder = if (recursive) b else b.delimiter("/")
      builder.build()
    }
    s3.listObjectsV2Paginator(request).toStream.flatMap { ol =>
      val fDirs =
        ol.commonPrefixes().asScala.toList.flatMap(cp => Option(cp.prefix())).traverse[F, Path[S3Blob]] { prefix =>
          if (expectTrailingSlashFiles) {
            Async[F].fromCompletableFuture(
              F.delay(s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(prefix).build()))
            ).map { resp =>
              Path(prefix).as(S3Blob(bucket, prefix, new S3MetaInfo.HeadObjectResponseMetaInfo(resp).some))
            }
              .recover {
                case _: NoSuchKeyException =>
                  Path(prefix).as(S3Blob(bucket, prefix, None))
              }
          } else {
            Path(prefix).as(S3Blob(bucket, prefix, None)).pure[F]
          }
        }

      val fFiles = ol.contents().asScala.toList.traverse[F, Path[S3Blob]] { s3Object =>
        if (fullMetadata) {
          Async[F].fromCompletableFuture(
            F.delay(s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(s3Object.key()).build()))
          ).map { resp =>
            Path(s3Object.key()).as(S3Blob(
              bucket,
              s3Object.key(),
              new S3MetaInfo.HeadObjectResponseMetaInfo(resp).some
            ))
          }
        } else {
          Path(s3Object.key()).as(S3Blob(bucket, s3Object.key(), new S3MetaInfo.S3ObjectMetaInfo(s3Object).some)).pure[F]
        }
      }
      Stream.eval(fDirs).flatMap(dirs => Stream(dirs: _*)) ++ Stream
        .eval(fFiles)
        .flatMap(files => Stream(files: _*))
    }
  }

  private def putKnownSize(
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo],
    size: Long,
    in: Stream[F, Byte]
  ): Stream[F, Unit] =
    Stream.resource(in.chunks.map(chunk => chunk.toByteBuffer).toUnicastPublisher).flatMap { publisher =>
      val request     = S3Store.putObjectRequest(sseAlgorithm, objectAcl)(bucket, key, meta, size)
      val requestBody = AsyncRequestBody.fromPublisher(publisher)
      Stream.eval(Async[F].fromCompletableFuture(F.delay(s3.putObject(request, requestBody))).void)
    }

  private def putUnknownSize(
    bucket: String,
    key: String,
    meta: Option[S3MetaInfo],
    in: Stream[F, Byte]
  ): Stream[F, Unit] =
    in.pull
      .unconsN(bufferSize, allowFewer = true)
      .flatMap {
        case None => Pull.done
        case Some((chunk, _)) if chunk.size < bufferSize =>
          val request     = S3Store.putObjectRequest(sseAlgorithm, objectAcl)(bucket, key, meta, chunk.size.toLong)
          val requestBody = AsyncRequestBody.fromByteBuffer(chunk.toByteBuffer)
          Pull.eval(Async[F].fromCompletableFuture(F.delay(s3.putObject(request, requestBody))).void)
        case Some((chunk, rest)) =>
          val request: CreateMultipartUploadRequest = {
            val builder = CreateMultipartUploadRequest.builder()
            val withAcl = objectAcl.fold(builder)(builder.acl)
            val withSSE = sseAlgorithm.fold(withAcl)(withAcl.serverSideEncryption)
            meta
              .fold(withSSE)(S3MetaInfo.createMultipartUploadRequest(withSSE))
              .bucket(bucket)
              .key(key)
              .build()
          }

          val makeRequest: F[CreateMultipartUploadResponse] =
            Async[F].fromCompletableFuture(F.delay(s3.createMultipartUpload(request)))

          Pull.eval(makeRequest).flatMap { muResp =>
            val abortReq: AbortMultipartUploadRequest =
              AbortMultipartUploadRequest.builder().bucket(bucket).key(key).uploadId(muResp.uploadId()).build()
            val uploadPartReqBuilder: UploadPartRequest.Builder = meta
              .fold(UploadPartRequest.builder())(S3MetaInfo.uploadPartRequest)
              .bucket(bucket)
              .key(key)
              .contentLength(bufferSize.toLong)
              .uploadId(muResp.uploadId())
            val completeReqBuilder = CompleteMultipartUploadRequest
              .builder()
              .bucket(bucket)
              .key(key)
              .uploadId(muResp.uploadId())

            val byteBuffer = ByteBuffer.allocateDirect(bufferSize)

            def mkNewPartResource(part: Int, size: Option[Long] = None): Resource[F, F[CompletedPart]] =
              Resource
                .makeCase {
                  val cf       = new CompletableFuture[ByteBuffer]()
                  val withPart = uploadPartReqBuilder.partNumber(part)
                  val req      = size.fold(withPart)(newSize => withPart.contentLength(newSize)).build()
                  val fBody    = F.delay(AsyncRequestBody.fromPublisher(CompletableFuturePublisher.from(cf)))
                  val done = for {
                    body <- fBody
                    resp <- Async[F].fromCompletableFuture(F.delay(s3.uploadPart(req, body)))
                    _    <- F.delay(byteBuffer.clear())
                  } yield CompletedPart.builder().eTag(resp.eTag()).partNumber(part).build()
                  F.delay((cf, done))
                } {
                  case ((cf, _), Resource.ExitCase.Succeeded) =>
                    F.delay { byteBuffer.flip(); cf.complete(byteBuffer) }.void
                  case ((cf, _), Resource.ExitCase.Errored(e)) =>
                    F.delay(cf.completeExceptionally(e)).void
                  case ((cf, _), Resource.ExitCase.Canceled) =>
                    F.delay(cf.cancel(true)).void
                }
                .map(_._2)

            Stream
              .resource(Hotswap(mkNewPartResource(1)))
              .pull
              .headOrError
              .flatMap {
                case (hotswap, completion) =>
                  Pull
                    .eval(
                      S3Store
                        .multipartGo(
                          S3Store.MultipartState(1, 0L),
                          rest.cons(chunk),
                          byteBuffer,
                          completion,
                          hotswap,
                          mkNewPartResource
                        )
                        .stream
                        .compile
                        .toList
                    )
                    .flatMap { parts =>
                      val request =
                        completeReqBuilder
                          .multipartUpload(CompletedMultipartUpload.builder().parts(parts.asJava).build())
                          .build()
                      Pull.eval(Async[F].fromCompletableFuture(F.delay(s3.completeMultipartUpload(request)))).void
                    }
              }
              .onError {
                case _ =>
                  Pull.eval(Async[F].fromCompletableFuture(F.delay(s3.abortMultipartUpload(abortReq)))).void
              }
          }
      }
      .stream

  override def stat(url: Url[Authority.Bucket]): Stream[F, Path[S3Blob]] =
    Stream.eval(Async[F].fromCompletableFuture(
      F.delay(s3.headObject(HeadObjectRequest.builder().bucket(url.bucket.show).key(url.path.relative.show).build()))
    ).map { resp =>
      Path.of(
        url.path.show,
        S3Blob(url.bucket.show, url.path.relative.show, new S3MetaInfo.HeadObjectResponseMetaInfo(resp).some)
      ).some
    }
      .recoverWith {
        case e: CompletionException =>
          Option(e.getCause) match {
            case Some(_: NoSuchKeyException) =>
              val meta = (!url.path.show.endsWith("/")).guard[Option].as(S3MetaInfo.const()).filterNot(_ =>
                defaultTrailingSlashFiles
              )
              Path.of(url.path.show, S3Blob(url.bucket.show, url.path.relative.show, meta)).some.pure
            case _ => Option(e.getCause).getOrElse(e).raiseError
          }
      }).unNone

  override def widen(implicit ME: MonadError[F, Throwable]): Store[F, Authority.Standard, S3Blob] =
    new DelegatingStore[F, Authority.Standard, S3Blob](Left(this))
}

object S3Store {
  def apply[F[_]: Async](
    s3: S3AsyncClient,
    objectAcl: Option[ObjectCannedACL] = None,
    sseAlgorithm: Option[String] = None,
    defaultFullMetadata: Boolean = false,
    defaultTrailingSlashFiles: Boolean = false,
    bufferSize: Int = S3Store.multiUploadDefaultPartSize.toInt,
    queueSize: Int = 32
  ): F[S3Store[F]] =
    if (bufferSize < S3Store.multiUploadMinimumPartSize)
      new IllegalArgumentException(s"Buffer size must be at least ${S3Store.multiUploadMinimumPartSize}").raiseError
    else
      Async[F].catchNonFatal {
        new S3Store(
          s3,
          objectAcl,
          sseAlgorithm,
          defaultFullMetadata,
          defaultTrailingSlashFiles,
          bufferSize,
          queueSize
        )
      }

  private def putObjectRequest(
    sseAlgorithm: Option[String],
    objectAcl: Option[ObjectCannedACL]
  )(bucket: String, key: String, meta: Option[S3MetaInfo], size: Long): PutObjectRequest = {
    val builder = PutObjectRequest.builder()
    val withAcl = objectAcl.fold(builder)(builder.acl)
    val withSSE = sseAlgorithm.fold(withAcl)(withAcl.serverSideEncryption)
    meta
      .fold(withSSE)(S3MetaInfo.putObjectRequest(withSSE))
      .contentLength(size)
      .bucket(bucket)
      .key(key)
      .build()
  }

  private val mb: Int = 1024 * 1024

  /** @see https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
    */
  private val multiUploadMinimumPartSize: Long = 5L * mb
  private val multiUploadDefaultPartSize: Long = 500L * mb
  private val maxTotalSize: Long               = 5000000L * mb

  private case class MultipartState(partNumber: Int, totalBytes: Long)

  private def multipartGo[F[_]](
    state: MultipartState,
    stream: Stream[F, Byte],
    byteBuffer: ByteBuffer,
    completion: F[CompletedPart],
    hotswap: Hotswap[F, F[CompletedPart]],
    newPart: (Int, Option[Long]) => Resource[F, F[CompletedPart]]
  )(implicit F: Sync[F]): Pull[F, CompletedPart, Unit] = {
    stream.pull.unconsLimit(byteBuffer.capacity() - byteBuffer.position()).flatMap {
      case Some((hd, tl)) =>
        val newTotal = state.totalBytes + hd.size
        if (newTotal > maxTotalSize) {
          Pull.raiseError(
            new IllegalArgumentException(
              s"S3 doesn't support files larger than 5 TB, stream has at least $newTotal bytes"
            )
          )
        } else {
          Pull
            .eval(F.delay(byteBuffer.put(hd.toByteBuffer)))
            .flatMap { _ =>
              if (byteBuffer.position() < byteBuffer.capacity()) {
                multipartGo(
                  state.copy(totalBytes = newTotal),
                  stream = tl,
                  byteBuffer = byteBuffer,
                  completion = completion,
                  hotswap = hotswap,
                  newPart = newPart
                )
              } else {
                Pull
                  .eval(hotswap.swap(newPart(state.partNumber + 1, None)))
                  .flatMap { newCompletion =>
                    Pull.eval(completion).flatMap { cp =>
                      Pull.output1[F, CompletedPart](cp) >>
                        multipartGo(
                          MultipartState(state.partNumber + 1, newTotal),
                          stream = tl,
                          byteBuffer = byteBuffer,
                          completion = newCompletion,
                          hotswap = hotswap,
                          newPart = newPart
                        )
                    }
                  }
              }
            }
        }
      case None =>
        val currentBytes = byteBuffer.position()
        if (currentBytes > 0) {
          Pull
            .eval(
              hotswap
                .swap(newPart(state.partNumber, Some(currentBytes.toLong)))
                .flatMap { newCompletion => hotswap.clear >> F.delay(byteBuffer.limit(currentBytes)) >> newCompletion }
            )
            .flatMap(Pull.output1)
        } else {
          Pull.done
        }
    }
  }
}
