package lightdb.transaction

import lightdb.doc.Document

trait TransactionManagerImplicits {
  // Tx2
  implicit def transactionA[A <: Document[A], B <: Document[B]](implicit tx: Tx2[A, B]): Transaction[A] = tx.ta
  implicit def transactionB[A <: Document[A], B <: Document[B]](implicit tx: Tx2[A, B]): Transaction[B] = tx.tb

  // Tx3
  implicit def transactionA[A <: Document[A], B <: Document[B], C <: Document[C]](implicit tx: Tx3[A, B, C]): Transaction[A] = tx.ta
  implicit def transactionB[A <: Document[A], B <: Document[B], C <: Document[C]](implicit tx: Tx3[A, B, C]): Transaction[B] = tx.tb
  implicit def transactionC[A <: Document[A], B <: Document[B], C <: Document[C]](implicit tx: Tx3[A, B, C]): Transaction[C] = tx.tc

  // Tx4
  implicit def transactionA[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D]](implicit tx: Tx4[A, B, C, D]): Transaction[A] = tx.ta
  implicit def transactionB[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D]](implicit tx: Tx4[A, B, C, D]): Transaction[B] = tx.tb
  implicit def transactionC[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D]](implicit tx: Tx4[A, B, C, D]): Transaction[C] = tx.tc
  implicit def transactionD[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D]](implicit tx: Tx4[A, B, C, D]): Transaction[D] = tx.td

  // Tx5
  implicit def transactionA[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E]](implicit tx: Tx5[A, B, C, D, E]): Transaction[A] = tx.ta
  implicit def transactionB[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E]](implicit tx: Tx5[A, B, C, D, E]): Transaction[B] = tx.tb
  implicit def transactionC[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E]](implicit tx: Tx5[A, B, C, D, E]): Transaction[C] = tx.tc
  implicit def transactionD[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E]](implicit tx: Tx5[A, B, C, D, E]): Transaction[D] = tx.td
  implicit def transactionE[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E]](implicit tx: Tx5[A, B, C, D, E]): Transaction[E] = tx.te

  // Tx6
  implicit def transactionA[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F]](implicit tx: Tx6[A, B, C, D, E, F]): Transaction[A] = tx.ta
  implicit def transactionB[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F]](implicit tx: Tx6[A, B, C, D, E, F]): Transaction[B] = tx.tb
  implicit def transactionC[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F]](implicit tx: Tx6[A, B, C, D, E, F]): Transaction[C] = tx.tc
  implicit def transactionD[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F]](implicit tx: Tx6[A, B, C, D, E, F]): Transaction[D] = tx.td
  implicit def transactionE[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F]](implicit tx: Tx6[A, B, C, D, E, F]): Transaction[E] = tx.te
  implicit def transactionF[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F]](implicit tx: Tx6[A, B, C, D, E, F]): Transaction[F] = tx.tf

  // Tx7
  implicit def transactionA[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G]](implicit tx: Tx7[A, B, C, D, E, F, G]): Transaction[A] = tx.ta
  implicit def transactionB[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G]](implicit tx: Tx7[A, B, C, D, E, F, G]): Transaction[B] = tx.tb
  implicit def transactionC[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G]](implicit tx: Tx7[A, B, C, D, E, F, G]): Transaction[C] = tx.tc
  implicit def transactionD[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G]](implicit tx: Tx7[A, B, C, D, E, F, G]): Transaction[D] = tx.td
  implicit def transactionE[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G]](implicit tx: Tx7[A, B, C, D, E, F, G]): Transaction[E] = tx.te
  implicit def transactionF[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G]](implicit tx: Tx7[A, B, C, D, E, F, G]): Transaction[F] = tx.tf
  implicit def transactionG[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G]](implicit tx: Tx7[A, B, C, D, E, F, G]): Transaction[G] = tx.tg

  // Tx8
  implicit def transactionA[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H]](implicit tx: Tx8[A, B, C, D, E, F, G, H]): Transaction[A] = tx.ta
  implicit def transactionB[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H]](implicit tx: Tx8[A, B, C, D, E, F, G, H]): Transaction[B] = tx.tb
  implicit def transactionC[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H]](implicit tx: Tx8[A, B, C, D, E, F, G, H]): Transaction[C] = tx.tc
  implicit def transactionD[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H]](implicit tx: Tx8[A, B, C, D, E, F, G, H]): Transaction[D] = tx.td
  implicit def transactionE[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H]](implicit tx: Tx8[A, B, C, D, E, F, G, H]): Transaction[E] = tx.te
  implicit def transactionF[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H]](implicit tx: Tx8[A, B, C, D, E, F, G, H]): Transaction[F] = tx.tf
  implicit def transactionG[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H]](implicit tx: Tx8[A, B, C, D, E, F, G, H]): Transaction[G] = tx.tg
  implicit def transactionH[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H]](implicit tx: Tx8[A, B, C, D, E, F, G, H]): Transaction[H] = tx.th

  // Tx9
  implicit def transactionA[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I]](implicit tx: Tx9[A, B, C, D, E, F, G, H, I]): Transaction[A] = tx.ta
  implicit def transactionB[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I]](implicit tx: Tx9[A, B, C, D, E, F, G, H, I]): Transaction[B] = tx.tb
  implicit def transactionC[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I]](implicit tx: Tx9[A, B, C, D, E, F, G, H, I]): Transaction[C] = tx.tc
  implicit def transactionD[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I]](implicit tx: Tx9[A, B, C, D, E, F, G, H, I]): Transaction[D] = tx.td
  implicit def transactionE[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I]](implicit tx: Tx9[A, B, C, D, E, F, G, H, I]): Transaction[E] = tx.te
  implicit def transactionF[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I]](implicit tx: Tx9[A, B, C, D, E, F, G, H, I]): Transaction[F] = tx.tf
  implicit def transactionG[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I]](implicit tx: Tx9[A, B, C, D, E, F, G, H, I]): Transaction[G] = tx.tg
  implicit def transactionH[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I]](implicit tx: Tx9[A, B, C, D, E, F, G, H, I]): Transaction[H] = tx.th
  implicit def transactionI[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I]](implicit tx: Tx9[A, B, C, D, E, F, G, H, I]): Transaction[I] = tx.ti

  // Tx10
  implicit def transactionA[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I], J <: Document[J]](implicit tx: Tx10[A, B, C, D, E, F, G, H, I, J]): Transaction[A] = tx.ta
  implicit def transactionB[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I], J <: Document[J]](implicit tx: Tx10[A, B, C, D, E, F, G, H, I, J]): Transaction[B] = tx.tb
  implicit def transactionC[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I], J <: Document[J]](implicit tx: Tx10[A, B, C, D, E, F, G, H, I, J]): Transaction[C] = tx.tc
  implicit def transactionD[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I], J <: Document[J]](implicit tx: Tx10[A, B, C, D, E, F, G, H, I, J]): Transaction[D] = tx.td
  implicit def transactionE[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I], J <: Document[J]](implicit tx: Tx10[A, B, C, D, E, F, G, H, I, J]): Transaction[E] = tx.te
  implicit def transactionF[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I], J <: Document[J]](implicit tx: Tx10[A, B, C, D, E, F, G, H, I, J]): Transaction[F] = tx.tf
  implicit def transactionG[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I], J <: Document[J]](implicit tx: Tx10[A, B, C, D, E, F, G, H, I, J]): Transaction[G] = tx.tg
  implicit def transactionH[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I], J <: Document[J]](implicit tx: Tx10[A, B, C, D, E, F, G, H, I, J]): Transaction[H] = tx.th
  implicit def transactionI[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I], J <: Document[J]](implicit tx: Tx10[A, B, C, D, E, F, G, H, I, J]): Transaction[I] = tx.ti
  implicit def transactionJ[A <: Document[A], B <: Document[B], C <: Document[C], D <: Document[D], E <: Document[E], F <: Document[F], G <: Document[G], H <: Document[H], I <: Document[I], J <: Document[J]](implicit tx: Tx10[A, B, C, D, E, F, G, H, I, J]): Transaction[J] = tx.tj
}