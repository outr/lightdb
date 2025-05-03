package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.Store
import rapid.Task

class TransactionManager {
  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1],
    D2 <: Document[D2], M2 <: DocumentModel[D2],
    Return
  ](s1: Store[D1, M1], s2: Store[D2, M2])
   (f: (Transaction[D1, M1], Transaction[D2, M2]) => Task[Return]): Task[Return] = for {
    t1 <- s1.transaction.create(None)
    t2 <- s2.transaction.create(None)
    r <- f(t1, t2).guarantee(s1.transaction.release(t1)
      .and(s2.transaction.release(t2)).unit)
  } yield r

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1],
    D2 <: Document[D2], M2 <: DocumentModel[D2],
    D3 <: Document[D3], M3 <: DocumentModel[D3],
    Return
  ](s1: Store[D1, M1], s2: Store[D2, M2], s3: Store[D3, M3])
   (f: (Transaction[D1, M1], Transaction[D2, M2], Transaction[D3, M3]) => Task[Return]): Task[Return] = for {
    t1 <- s1.transaction.create(None)
    t2 <- s2.transaction.create(None)
    t3 <- s3.transaction.create(None)
    r <- f(t1, t2, t3).guarantee(s1.transaction.release(t1)
      .and(s2.transaction.release(t2))
      .and(s3.transaction.release(t3)).unit)
  } yield r

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1],
    D2 <: Document[D2], M2 <: DocumentModel[D2],
    D3 <: Document[D3], M3 <: DocumentModel[D3],
    D4 <: Document[D4], M4 <: DocumentModel[D4],
    Return
  ](s1: Store[D1, M1], s2: Store[D2, M2], s3: Store[D3, M3], s4: Store[D4, M4])
   (f: (Transaction[D1, M1], Transaction[D2, M2], Transaction[D3, M3], Transaction[D4, M4]) => Task[Return]): Task[Return] = for {
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
    D1 <: Document[D1], M1 <: DocumentModel[D1],
    D2 <: Document[D2], M2 <: DocumentModel[D2],
    D3 <: Document[D3], M3 <: DocumentModel[D3],
    D4 <: Document[D4], M4 <: DocumentModel[D4],
    D5 <: Document[D5], M5 <: DocumentModel[D5],
    Return
  ](s1: Store[D1, M1], s2: Store[D2, M2], s3: Store[D3, M3], s4: Store[D4, M4], s5: Store[D5, M5])
   (f: (Transaction[D1, M1], Transaction[D2, M2], Transaction[D3, M3], Transaction[D4, M4], Transaction[D5, M5]) => Task[Return]): Task[Return] = for {
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
    D1 <: Document[D1], M1 <: DocumentModel[D1],
    D2 <: Document[D2], M2 <: DocumentModel[D2],
    D3 <: Document[D3], M3 <: DocumentModel[D3],
    D4 <: Document[D4], M4 <: DocumentModel[D4],
    D5 <: Document[D5], M5 <: DocumentModel[D5],
    D6 <: Document[D6], M6 <: DocumentModel[D6],
    Return
  ](s1: Store[D1, M1], s2: Store[D2, M2], s3: Store[D3, M3], s4: Store[D4, M4], s5: Store[D5, M5], s6: Store[D6, M6])
   (f: (Transaction[D1, M1], Transaction[D2, M2], Transaction[D3, M3], Transaction[D4, M4], Transaction[D5, M5], Transaction[D6, M6]) => Task[Return]): Task[Return] = for {
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
    D1 <: Document[D1], M1 <: DocumentModel[D1],
    D2 <: Document[D2], M2 <: DocumentModel[D2],
    D3 <: Document[D3], M3 <: DocumentModel[D3],
    D4 <: Document[D4], M4 <: DocumentModel[D4],
    D5 <: Document[D5], M5 <: DocumentModel[D5],
    D6 <: Document[D6], M6 <: DocumentModel[D6],
    D7 <: Document[D7], M7 <: DocumentModel[D7],
    Return
  ](s1: Store[D1, M1], s2: Store[D2, M2], s3: Store[D3, M3], s4: Store[D4, M4], s5: Store[D5, M5], s6: Store[D6, M6], s7: Store[D7, M7])
   (f: (Transaction[D1, M1], Transaction[D2, M2], Transaction[D3, M3], Transaction[D4, M4], Transaction[D5, M5], Transaction[D6, M6], Transaction[D7, M7]) => Task[Return]): Task[Return] = for {
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
    D1 <: Document[D1], M1 <: DocumentModel[D1],
    D2 <: Document[D2], M2 <: DocumentModel[D2],
    D3 <: Document[D3], M3 <: DocumentModel[D3],
    D4 <: Document[D4], M4 <: DocumentModel[D4],
    D5 <: Document[D5], M5 <: DocumentModel[D5],
    D6 <: Document[D6], M6 <: DocumentModel[D6],
    D7 <: Document[D7], M7 <: DocumentModel[D7],
    D8 <: Document[D8], M8 <: DocumentModel[D8],
    Return
  ](s1: Store[D1, M1], s2: Store[D2, M2], s3: Store[D3, M3], s4: Store[D4, M4], s5: Store[D5, M5], s6: Store[D6, M6], s7: Store[D7, M7], s8: Store[D8, M8])
   (f: (Transaction[D1, M1], Transaction[D2, M2], Transaction[D3, M3], Transaction[D4, M4], Transaction[D5, M5], Transaction[D6, M6], Transaction[D7, M7], Transaction[D8, M8]) => Task[Return]): Task[Return] = for {
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
    D1 <: Document[D1], M1 <: DocumentModel[D1],
    D2 <: Document[D2], M2 <: DocumentModel[D2],
    D3 <: Document[D3], M3 <: DocumentModel[D3],
    D4 <: Document[D4], M4 <: DocumentModel[D4],
    D5 <: Document[D5], M5 <: DocumentModel[D5],
    D6 <: Document[D6], M6 <: DocumentModel[D6],
    D7 <: Document[D7], M7 <: DocumentModel[D7],
    D8 <: Document[D8], M8 <: DocumentModel[D8],
    D9 <: Document[D9], M9 <: DocumentModel[D9],
    Return
  ](s1: Store[D1, M1], s2: Store[D2, M2], s3: Store[D3, M3], s4: Store[D4, M4], s5: Store[D5, M5], s6: Store[D6, M6], s7: Store[D7, M7], s8: Store[D8, M8], s9: Store[D9, M9])
   (f: (Transaction[D1, M1], Transaction[D2, M2], Transaction[D3, M3], Transaction[D4, M4], Transaction[D5, M5], Transaction[D6, M6], Transaction[D7, M7], Transaction[D8, M8], Transaction[D9, M9]) => Task[Return]): Task[Return] = for {
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
    D1 <: Document[D1], M1 <: DocumentModel[D1],
    D2 <: Document[D2], M2 <: DocumentModel[D2],
    D3 <: Document[D3], M3 <: DocumentModel[D3],
    D4 <: Document[D4], M4 <: DocumentModel[D4],
    D5 <: Document[D5], M5 <: DocumentModel[D5],
    D6 <: Document[D6], M6 <: DocumentModel[D6],
    D7 <: Document[D7], M7 <: DocumentModel[D7],
    D8 <: Document[D8], M8 <: DocumentModel[D8],
    D9 <: Document[D9], M9 <: DocumentModel[D9],
    D10 <: Document[D10], M10 <: DocumentModel[D10],
    Return
  ](s1: Store[D1, M1], s2: Store[D2, M2], s3: Store[D3, M3], s4: Store[D4, M4], s5: Store[D5, M5], s6: Store[D6, M6], s7: Store[D7, M7], s8: Store[D8, M8], s9: Store[D9, M9], s10: Store[D10, M10])
   (f: (Transaction[D1, M1], Transaction[D2, M2], Transaction[D3, M3], Transaction[D4, M4], Transaction[D5, M5], Transaction[D6, M6], Transaction[D7, M7], Transaction[D8, M8], Transaction[D9, M9], Transaction[D10, M10]) => Task[Return]): Task[Return] = for {
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