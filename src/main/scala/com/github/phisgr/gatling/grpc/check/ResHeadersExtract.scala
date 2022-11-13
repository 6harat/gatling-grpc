package com.github.phisgr.gatling.grpc.check

import com.github.phisgr.gatling.grpc.check.GrpcResponse.GrpcStreamEnd
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.check.{CheckMaterializer, CountCriterionExtractor, DefaultMultipleFindCheckBuilder, Extractor, FindAllCriterionExtractor, FindCriterionExtractor, Preparer}
import io.gatling.core.session.{Expression, ExpressionSuccessWrapper}
import io.grpc.Metadata

import scala.collection.JavaConverters._

private[gatling] object ResHeadersExtract {

  def extract[T](key: Metadata.Key[T]): DefaultMultipleFindCheckBuilder[ResHeadersExtract, Metadata, T] =
    new DefaultMultipleFindCheckBuilder[ResHeadersExtract, Metadata, T](
      displayActualValue = true
    ) {
      // The use of the iterable returned by `getAll` involves deserializing unused elements
      // except for `find(0)` and `findAll`
      override def findExtractor(occurrence: Int): Expression[Extractor[Metadata, T]] =
        new FindCriterionExtractor[Metadata, String, T](
          checkName = "resHeaders",
          criterion = key.name(),
          occurrence = occurrence,
          extractor = { metadata =>
            Option(metadata).flatMap(m => Option(m.getAll(key))).flatMap { iterable =>
              val iterator = iterable.iterator().asScala
              iterator.drop(occurrence)
              if (iterator.hasNext) Some(iterator.next()) else None
            }.success
          }
        ).expressionSuccess
      override def findAllExtractor: Expression[Extractor[Metadata, Seq[T]]] =
        new FindAllCriterionExtractor[Metadata, String, T](
          checkName = "resHeaders",
          criterion = key.name(),
          extractor = metadata => Option(metadata).flatMap(m => Option(m.getAll(key))).map(_.asScala.toSeq).success
        ).expressionSuccess
      override def countExtractor: Expression[Extractor[Metadata, Int]] =
        new CountCriterionExtractor[Metadata, String](
          checkName = "resHeaders",
          criterion = key.name(),
          extractor = metadata => Option(metadata).flatMap(m => Option(m.getAll(key))).map(m => m match {
            case null => 0
            case iterable => iterable.asScala.size
          }).success
        ).expressionSuccess
    }

  object Materializer extends CheckMaterializer[ResHeadersExtract, GrpcCheck[Any], GrpcResponse[Any], Metadata](
    specializer = GrpcCheck(_, GrpcCheck.ResHeaders)
  ) {
    override protected def preparer: Preparer[GrpcResponse[Any], Metadata] = _.resHeaders.success
  }

  object StreamMaterializer extends CheckMaterializer[ResHeadersExtract, StreamCheck[GrpcStreamEnd], GrpcStreamEnd, Metadata](
    specializer = StreamCheck(_, GrpcCheck.ResHeaders)
  ) {
    override protected def preparer: Preparer[GrpcStreamEnd, Metadata] = _.resHeaders.success
  }

}

// phantom type for implicit materializer resolution
trait ResHeadersExtract
