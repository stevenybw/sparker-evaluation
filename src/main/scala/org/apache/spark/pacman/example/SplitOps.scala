package org.apache.spark.pacman.example

import breeze.linalg.DenseVector

import scala.reflect.ClassTag

object SplitOps {
  val denseVectorSplitOpLong = (vec: DenseVector[Long], chunk_idx: Int, num_chunks: Int) => {
    val blockSize = vec.length / num_chunks
    val beginPos = chunk_idx * blockSize
    val endPos = if (chunk_idx == num_chunks - 1) vec.length else (chunk_idx + 1) * blockSize
    vec.slice(beginPos, endPos)
  }

  val denseVectorConcatOpLong = (vectors: Seq[DenseVector[Long]]) => {
    DenseVector.vertcat(vectors: _*)
  }

  val denseVectorConcatOpDouble = (vectors: Seq[DenseVector[Double]]) => {
    DenseVector.vertcat(vectors: _*)
  }

  def denseVectorSplitOp[T: ClassTag](vec: DenseVector[T], chunk_idx: Int, num_chunks: Int): DenseVector[T] = {
    val blockSize = vec.length / num_chunks
    val beginPos = chunk_idx * blockSize
    val endPos = if (chunk_idx == num_chunks - 1) vec.length else (chunk_idx + 1) * blockSize
    vec.slice(beginPos, endPos)
  }
}
