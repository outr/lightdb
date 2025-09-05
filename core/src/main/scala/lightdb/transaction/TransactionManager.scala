package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.Store
import rapid._

class TransactionManager {
  def apply[
    Doc <: Document[Doc],
    Model <: DocumentModel[Doc],
    Tx,
    S <: Store[Doc, Model] { type TX = Tx },
    Return
  ](list: List[S])(f: List[Tx] => Task[Return]): Task[Return] = {
    final case class Acquired(tx: Tx, release: Task[Unit])

    for {
      acquired <- list.map { s =>
        s.transaction.create(None).map { tx0 =>
          Acquired(tx = tx0, release = s.transaction.release(tx0))
        }
      }.tasks
      r <- f(acquired.map(_.tx))
        // always release (reverse order is safer for nested locks)
        .guarantee(acquired.reverse.map(_.release).tasks.unit)
    } yield r
  }

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    Return
  ](s1: S1, s2: S2)
   (f: (s1.TX, s2.TX) => Task[Return]): Task[Return] = for {
    t1 <- s1.transaction.create(None)
    t2 <- s2.transaction.create(None)
    r <- f(t1, t2).guarantee(s1.transaction.release(t1)
      .and(s2.transaction.release(t2)).unit)
  } yield r

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    D3 <: Document[D3], M3 <: DocumentModel[D3], S3 <: Store[D3, M3],
    Return
  ](s1: S1, s2: S2, s3: S3)
   (f: (s1.TX, s2.TX, s3.TX) => Task[Return]): Task[Return] = for {
    t1 <- s1.transaction.create(None)
    t2 <- s2.transaction.create(None)
    t3 <- s3.transaction.create(None)
    r <- f(t1, t2, t3).guarantee(s1.transaction.release(t1)
      .and(s2.transaction.release(t2))
      .and(s3.transaction.release(t3)).unit)
  } yield r

  // This is the original third method, keeping it as is
  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    D3 <: Document[D3], M3 <: DocumentModel[D3], S3 <: Store[D3, M3],
    D4 <: Document[D4], M4 <: DocumentModel[D4], S4 <: Store[D4, M4],
    Return
  ](s1: S1, s2: S2, s3: S3, s4: S4)
   (f: (s1.TX, s2.TX, s3.TX, s4.TX) => Task[Return]): Task[Return] = for {
    t1 <- s1.transaction.create(None)
    t2 <- s2.transaction.create(None)
    t3 <- s3.transaction.create(None)
    t4 <- s4.transaction.create(None)
    r <- f(t1, t2, t3, t4).guarantee(s1.transaction.release(t1)
      .and(s2.transaction.release(t2))
      .and(s3.transaction.release(t3))
      .and(s4.transaction.release(t4)).unit)
  } yield r

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    D3 <: Document[D3], M3 <: DocumentModel[D3], S3 <: Store[D3, M3],
    D4 <: Document[D4], M4 <: DocumentModel[D4], S4 <: Store[D4, M4],
    D5 <: Document[D5], M5 <: DocumentModel[D5], S5 <: Store[D5, M5],
    Return
  ](s1: S1, s2: S2, s3: S3, s4: S4, s5: S5)
   (f: (s1.TX, s2.TX, s3.TX, s4.TX, s5.TX) => Task[Return]): Task[Return] = for {
    t1 <- s1.transaction.create(None)
    t2 <- s2.transaction.create(None)
    t3 <- s3.transaction.create(None)
    t4 <- s4.transaction.create(None)
    t5 <- s5.transaction.create(None)
    r <- f(t1, t2, t3, t4, t5).guarantee(s1.transaction.release(t1)
      .and(s2.transaction.release(t2))
      .and(s3.transaction.release(t3))
      .and(s4.transaction.release(t4))
      .and(s5.transaction.release(t5)).unit)
  } yield r

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    D3 <: Document[D3], M3 <: DocumentModel[D3], S3 <: Store[D3, M3],
    D4 <: Document[D4], M4 <: DocumentModel[D4], S4 <: Store[D4, M4],
    D5 <: Document[D5], M5 <: DocumentModel[D5], S5 <: Store[D5, M5],
    D6 <: Document[D6], M6 <: DocumentModel[D6], S6 <: Store[D6, M6],
    Return
  ](s1: S1, s2: S2, s3: S3, s4: S4, s5: S5, s6: S6)
   (f: (s1.TX, s2.TX, s3.TX, s4.TX, s5.TX, s6.TX) => Task[Return]): Task[Return] = for {
    t1 <- s1.transaction.create(None)
    t2 <- s2.transaction.create(None)
    t3 <- s3.transaction.create(None)
    t4 <- s4.transaction.create(None)
    t5 <- s5.transaction.create(None)
    t6 <- s6.transaction.create(None)
    r <- f(t1, t2, t3, t4, t5, t6).guarantee(s1.transaction.release(t1)
      .and(s2.transaction.release(t2))
      .and(s3.transaction.release(t3))
      .and(s4.transaction.release(t4))
      .and(s5.transaction.release(t5))
      .and(s6.transaction.release(t6)).unit)
  } yield r

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    D3 <: Document[D3], M3 <: DocumentModel[D3], S3 <: Store[D3, M3],
    D4 <: Document[D4], M4 <: DocumentModel[D4], S4 <: Store[D4, M4],
    D5 <: Document[D5], M5 <: DocumentModel[D5], S5 <: Store[D5, M5],
    D6 <: Document[D6], M6 <: DocumentModel[D6], S6 <: Store[D6, M6],
    D7 <: Document[D7], M7 <: DocumentModel[D7], S7 <: Store[D7, M7],
    Return
  ](s1: S1, s2: S2, s3: S3, s4: S4, s5: S5, s6: S6, s7: S7)
   (f: (s1.TX, s2.TX, s3.TX, s4.TX, s5.TX, s6.TX, s7.TX) => Task[Return]): Task[Return] = for {
    t1 <- s1.transaction.create(None)
    t2 <- s2.transaction.create(None)
    t3 <- s3.transaction.create(None)
    t4 <- s4.transaction.create(None)
    t5 <- s5.transaction.create(None)
    t6 <- s6.transaction.create(None)
    t7 <- s7.transaction.create(None)
    r <- f(t1, t2, t3, t4, t5, t6, t7).guarantee(s1.transaction.release(t1)
      .and(s2.transaction.release(t2))
      .and(s3.transaction.release(t3))
      .and(s4.transaction.release(t4))
      .and(s5.transaction.release(t5))
      .and(s6.transaction.release(t6))
      .and(s7.transaction.release(t7)).unit)
  } yield r

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    D3 <: Document[D3], M3 <: DocumentModel[D3], S3 <: Store[D3, M3],
    D4 <: Document[D4], M4 <: DocumentModel[D4], S4 <: Store[D4, M4],
    D5 <: Document[D5], M5 <: DocumentModel[D5], S5 <: Store[D5, M5],
    D6 <: Document[D6], M6 <: DocumentModel[D6], S6 <: Store[D6, M6],
    D7 <: Document[D7], M7 <: DocumentModel[D7], S7 <: Store[D7, M7],
    D8 <: Document[D8], M8 <: DocumentModel[D8], S8 <: Store[D8, M8],
    Return
  ](s1: S1, s2: S2, s3: S3, s4: S4, s5: S5, s6: S6, s7: S7, s8: S8)
   (f: (s1.TX, s2.TX, s3.TX, s4.TX, s5.TX, s6.TX, s7.TX, s8.TX) => Task[Return]): Task[Return] = for {
    t1 <- s1.transaction.create(None)
    t2 <- s2.transaction.create(None)
    t3 <- s3.transaction.create(None)
    t4 <- s4.transaction.create(None)
    t5 <- s5.transaction.create(None)
    t6 <- s6.transaction.create(None)
    t7 <- s7.transaction.create(None)
    t8 <- s8.transaction.create(None)
    r <- f(t1, t2, t3, t4, t5, t6, t7, t8).guarantee(s1.transaction.release(t1)
      .and(s2.transaction.release(t2))
      .and(s3.transaction.release(t3))
      .and(s4.transaction.release(t4))
      .and(s5.transaction.release(t5))
      .and(s6.transaction.release(t6))
      .and(s7.transaction.release(t7))
      .and(s8.transaction.release(t8)).unit)
  } yield r

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    D3 <: Document[D3], M3 <: DocumentModel[D3], S3 <: Store[D3, M3],
    D4 <: Document[D4], M4 <: DocumentModel[D4], S4 <: Store[D4, M4],
    D5 <: Document[D5], M5 <: DocumentModel[D5], S5 <: Store[D5, M5],
    D6 <: Document[D6], M6 <: DocumentModel[D6], S6 <: Store[D6, M6],
    D7 <: Document[D7], M7 <: DocumentModel[D7], S7 <: Store[D7, M7],
    D8 <: Document[D8], M8 <: DocumentModel[D8], S8 <: Store[D8, M8],
    D9 <: Document[D9], M9 <: DocumentModel[D9], S9 <: Store[D9, M9],
    Return
  ](s1: S1, s2: S2, s3: S3, s4: S4, s5: S5, s6: S6, s7: S7, s8: S8, s9: S9)
   (f: (s1.TX, s2.TX, s3.TX, s4.TX, s5.TX, s6.TX, s7.TX, s8.TX, s9.TX) => Task[Return]): Task[Return] = for {
    t1 <- s1.transaction.create(None)
    t2 <- s2.transaction.create(None)
    t3 <- s3.transaction.create(None)
    t4 <- s4.transaction.create(None)
    t5 <- s5.transaction.create(None)
    t6 <- s6.transaction.create(None)
    t7 <- s7.transaction.create(None)
    t8 <- s8.transaction.create(None)
    t9 <- s9.transaction.create(None)
    r <- f(t1, t2, t3, t4, t5, t6, t7, t8, t9).guarantee(s1.transaction.release(t1)
      .and(s2.transaction.release(t2))
      .and(s3.transaction.release(t3))
      .and(s4.transaction.release(t4))
      .and(s5.transaction.release(t5))
      .and(s6.transaction.release(t6))
      .and(s7.transaction.release(t7))
      .and(s8.transaction.release(t8))
      .and(s9.transaction.release(t9)).unit)
  } yield r

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    D3 <: Document[D3], M3 <: DocumentModel[D3], S3 <: Store[D3, M3],
    D4 <: Document[D4], M4 <: DocumentModel[D4], S4 <: Store[D4, M4],
    D5 <: Document[D5], M5 <: DocumentModel[D5], S5 <: Store[D5, M5],
    D6 <: Document[D6], M6 <: DocumentModel[D6], S6 <: Store[D6, M6],
    D7 <: Document[D7], M7 <: DocumentModel[D7], S7 <: Store[D7, M7],
    D8 <: Document[D8], M8 <: DocumentModel[D8], S8 <: Store[D8, M8],
    D9 <: Document[D9], M9 <: DocumentModel[D9], S9 <: Store[D9, M9],
    D10 <: Document[D10], M10 <: DocumentModel[D10], S10 <: Store[D10, M10],
    Return
  ](s1: S1, s2: S2, s3: S3, s4: S4, s5: S5, s6: S6, s7: S7, s8: S8, s9: S9, s10: S10)
   (f: (s1.TX, s2.TX, s3.TX, s4.TX, s5.TX, s6.TX, s7.TX, s8.TX, s9.TX, s10.TX) => Task[Return]): Task[Return] = for {
    t1 <- s1.transaction.create(None)
    t2 <- s2.transaction.create(None)
    t3 <- s3.transaction.create(None)
    t4 <- s4.transaction.create(None)
    t5 <- s5.transaction.create(None)
    t6 <- s6.transaction.create(None)
    t7 <- s7.transaction.create(None)
    t8 <- s8.transaction.create(None)
    t9 <- s9.transaction.create(None)
    t10 <- s10.transaction.create(None)
    r <- f(t1, t2, t3, t4, t5, t6, t7, t8, t9, t10).guarantee(s1.transaction.release(t1)
      .and(s2.transaction.release(t2))
      .and(s3.transaction.release(t3))
      .and(s4.transaction.release(t4))
      .and(s5.transaction.release(t5))
      .and(s6.transaction.release(t6))
      .and(s7.transaction.release(t7))
      .and(s8.transaction.release(t8))
      .and(s9.transaction.release(t9))
      .and(s10.transaction.release(t10)).unit)
  } yield r
}