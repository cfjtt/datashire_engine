package org.apache.spark.mllib.linalg.distributed

import org.apache.spark.mllib.linalg.{DenseMatrix, SparseMatrix}
import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM}
import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.Matrix

/**
  * Created by Administrator on 2017-08-24.
  */
object BlockMatrixUtil {

  def subtract(A:BlockMatrix,B:BlockMatrix):BlockMatrix={
      blockMap(A,B, (x: BM[Double], y: BM[Double]) => x - y)
  }

  private type MatrixBlock = ((Int, Int), Matrix) // ((blockRowIndex, blockColIndex), sub-matrix)

  def blockMap(A:BlockMatrix,other: BlockMatrix,
                binMap: (BM[Double], BM[Double]) => BM[Double]): BlockMatrix = {
    require(A.numRows() == other.numRows(), "Both matrices must have the same number of rows. " +
      s"A.numRows: ${A.numRows()}, B.numRows: ${other.numRows()}")
    require(A.numCols() == other.numCols(), "Both matrices must have the same number of columns. " +
      s"A.numCols: ${A.numCols()}, B.numCols: ${other.numCols()}")
    if (A.rowsPerBlock == other.rowsPerBlock && A.colsPerBlock == other.colsPerBlock) {
      val newBlocks = A.blocks.cogroup(other.blocks, A.createPartitioner())
        .map { case ((blockRowIndex, blockColIndex), (a, b)) =>
          if (a.size > 1 || b.size > 1) {
            throw new SparkException("There are multiple MatrixBlocks with indices: " +
              s"($blockRowIndex, $blockColIndex). Please remove them.")
          }
          if (a.isEmpty) {
            val zeroBlock = BM.zeros[Double](b.head.numRows, b.head.numCols)
            val result = binMap(zeroBlock, b.head.asBreeze)
            new MatrixBlock((blockRowIndex, blockColIndex), fromBreeze(result))
          } else if (b.isEmpty) {
            new MatrixBlock((blockRowIndex, blockColIndex), a.head)
          } else {
            val result = binMap(a.head.asBreeze, b.head.asBreeze)
            new MatrixBlock((blockRowIndex, blockColIndex), fromBreeze(result))
          }
        }
      new BlockMatrix(newBlocks, A.rowsPerBlock, A.colsPerBlock, A.numRows(),A.numCols())
    } else {
      throw new SparkException("Cannot perform on matrices with different block dimensions")
    }
  }

  /**
    * Creates a Matrix instance from a breeze matrix.
    * @param breeze a breeze matrix
    * @return a Matrix instance
    */
   def fromBreeze(breeze: BM[Double]): Matrix = {
    breeze match {
      case dm: BDM[Double] =>
        new DenseMatrix(dm.rows, dm.cols, dm.data, dm.isTranspose)
      case sm: BSM[Double] =>
        // There is no isTranspose flag for sparse matrices in Breeze
        val nsm = if (sm.rowIndices.length > sm.activeSize) {
          // This sparse matrix has trailing zeros.
          // Remove them by compacting the matrix.
          val csm = sm.copy
          csm.compact()
          csm
        } else {
          sm
        }
        new SparseMatrix(nsm.rows, nsm.cols, nsm.colPtrs, nsm.rowIndices, nsm.data)
      case _ =>
        throw new UnsupportedOperationException(
          s"Do not support conversion from type ${breeze.getClass.getName}.")
    }
  }

}
