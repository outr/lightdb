package lightdb.transaction

import lightdb.doc.Document
import lightdb.store.Store
import rapid.Task

class TransactionManager {
  def apply[A <: Document[A], B <: Document[B], Return](a: Store[A, _], b: Store[B, _])
                                                       (f: Tx2[A, B] => Task[Return]): Task[Return] = for {
    ta <- a.transaction.create()
    tb <- b.transaction.create()
    tx = Tx2(ta, tb)
    r <- f(tx).guarantee(a.transaction.release(ta).and(b.transaction.release(tb)).unit)
  } yield r

  def apply[A <: Document[A], B <: Document[B], C <: Document[C], Return](a: Store[A, _], b: Store[B, _], c: Store[C, _])
                                                                         (f: Tx3[A, B, C] => Task[Return]): Task[Return] = for {
    ta <- a.transaction.create()
    tb <- b.transaction.create()
    tc <- c.transaction.create()
    tx = Tx3(ta, tb, tc)
    r <- f(tx).guarantee(a.transaction.release(ta)
      .and(b.transaction.release(tb))
      .and(c.transaction.release(tc)).unit)
  } yield r

  def apply[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], Return](a: Store[A, _], b: Store[B, _], c: Store[C, _], d: Store[D, _])
                                                                                           (f: Tx4[A, B, C, D] => Task[Return]): Task[Return] = for {
    ta <- a.transaction.create()
    tb <- b.transaction.create()
    tc <- c.transaction.create()
    td <- d.transaction.create()
    tx = Tx4(ta, tb, tc, td)
    r <- f(tx).guarantee(a.transaction.release(ta)
      .and(b.transaction.release(tb))
      .and(c.transaction.release(tc))
      .and(d.transaction.release(td)).unit)
  } yield r

  def apply[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], Return](a: Store[A, _], b: Store[B, _], c: Store[C, _], d: Store[D, _], e: Store[E, _])
                                                                                                             (f: Tx5[A, B, C, D, E] => Task[Return]): Task[Return] = for {
    ta <- a.transaction.create()
    tb <- b.transaction.create()
    tc <- c.transaction.create()
    td <- d.transaction.create()
    te <- e.transaction.create()
    tx = Tx5(ta, tb, tc, td, te)
    r <- f(tx).guarantee(a.transaction.release(ta)
      .and(b.transaction.release(tb))
      .and(c.transaction.release(tc))
      .and(d.transaction.release(td))
      .and(e.transaction.release(te)).unit)
  } yield r

  def apply[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], Return](a: Store[A, _], b: Store[B, _], c: Store[C, _], d: Store[D, _], e: Store[E, _], fStore: Store[F, _])
                                                                                                                               (f: Tx6[A, B, C, D, E, F] => Task[Return]): Task[Return] = for {
    ta <- a.transaction.create()
    tb <- b.transaction.create()
    tc <- c.transaction.create()
    td <- d.transaction.create()
    te <- e.transaction.create()
    tf <- fStore.transaction.create()
    tx = Tx6(ta, tb, tc, td, te, tf)
    r <- f(tx).guarantee(a.transaction.release(ta)
      .and(b.transaction.release(tb))
      .and(c.transaction.release(tc))
      .and(d.transaction.release(td))
      .and(e.transaction.release(te))
      .and(fStore.transaction.release(tf)).unit)
  } yield r

  def apply[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], Return](a: Store[A, _], b: Store[B, _], c: Store[C, _], d: Store[D, _], e: Store[E, _], fStore: Store[F, _], g: Store[G, _])
                                                                                                                                                 (f: Tx7[A, B, C, D, E, F, G] => Task[Return]): Task[Return] = for {
    ta <- a.transaction.create()
    tb <- b.transaction.create()
    tc <- c.transaction.create()
    td <- d.transaction.create()
    te <- e.transaction.create()
    tf <- fStore.transaction.create()
    tg <- g.transaction.create()
    tx = Tx7(ta, tb, tc, td, te, tf, tg)
    r <- f(tx).guarantee(a.transaction.release(ta)
      .and(b.transaction.release(tb))
      .and(c.transaction.release(tc))
      .and(d.transaction.release(td))
      .and(e.transaction.release(te))
      .and(fStore.transaction.release(tf))
      .and(g.transaction.release(tg)).unit)
  } yield r

  def apply[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], Return](a: Store[A, _], b: Store[B, _], c: Store[C, _], d: Store[D, _], e: Store[E, _], fStore: Store[F, _], g: Store[G, _], h: Store[H, _])
                                                                                                                                                                   (f: Tx8[A, B, C, D, E, F, G, H] => Task[Return]): Task[Return] = for {
    ta <- a.transaction.create()
    tb <- b.transaction.create()
    tc <- c.transaction.create()
    td <- d.transaction.create()
    te <- e.transaction.create()
    tf <- fStore.transaction.create()
    tg <- g.transaction.create()
    th <- h.transaction.create()
    tx = Tx8(ta, tb, tc, td, te, tf, tg, th)
    r <- f(tx).guarantee(a.transaction.release(ta)
      .and(b.transaction.release(tb))
      .and(c.transaction.release(tc))
      .and(d.transaction.release(td))
      .and(e.transaction.release(te))
      .and(fStore.transaction.release(tf))
      .and(g.transaction.release(tg))
      .and(h.transaction.release(th)).unit)
  } yield r

  def apply[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I], Return](a: Store[A, _], b: Store[B, _], c: Store[C, _], d: Store[D, _], e: Store[E, _], fStore: Store[F, _], g: Store[G, _], h: Store[H, _], i: Store[I, _])
                                                                                                                                                                                     (f: Tx9[A, B, C, D, E, F, G, H, I] => Task[Return]): Task[Return] = for {
    ta <- a.transaction.create()
    tb <- b.transaction.create()
    tc <- c.transaction.create()
    td <- d.transaction.create()
    te <- e.transaction.create()
    tf <- fStore.transaction.create()
    tg <- g.transaction.create()
    th <- h.transaction.create()
    ti <- i.transaction.create()
    tx = Tx9(ta, tb, tc, td, te, tf, tg, th, ti)
    r <- f(tx).guarantee(a.transaction.release(ta)
      .and(b.transaction.release(tb))
      .and(c.transaction.release(tc))
      .and(d.transaction.release(td))
      .and(e.transaction.release(te))
      .and(fStore.transaction.release(tf))
      .and(g.transaction.release(tg))
      .and(h.transaction.release(th))
      .and(i.transaction.release(ti)).unit)
  } yield r

  def apply[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I], J <: Document[J], Return](a: Store[A, _], b: Store[B, _], c: Store[C, _], d: Store[D, _], e: Store[E, _], fStore: Store[F, _], g: Store[G, _], h: Store[H, _], i: Store[I, _], j: Store[J, _])
                                                                                                                                                                                                       (f: Tx10[A, B, C, D, E, F, G, H, I, J] => Task[Return]): Task[Return] = for {
    ta <- a.transaction.create()
    tb <- b.transaction.create()
    tc <- c.transaction.create()
    td <- d.transaction.create()
    te <- e.transaction.create()
    tf <- fStore.transaction.create()
    tg <- g.transaction.create()
    th <- h.transaction.create()
    ti <- i.transaction.create()
    tj <- j.transaction.create()
    tx = Tx10(ta, tb, tc, td, te, tf, tg, th, ti, tj)
    r <- f(tx).guarantee(a.transaction.release(ta)
      .and(b.transaction.release(tb))
      .and(c.transaction.release(tc))
      .and(d.transaction.release(td))
      .and(e.transaction.release(te))
      .and(fStore.transaction.release(tf))
      .and(g.transaction.release(tg))
      .and(h.transaction.release(th))
      .and(i.transaction.release(ti))
      .and(j.transaction.release(tj)).unit)
  } yield r
}