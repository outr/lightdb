package lightdb.transaction

import lightdb.doc.{Document, DocumentModel}
import lightdb.store.Store
import rapid.*

class TransactionManager {
  def apply[
    Doc <: Document[Doc],
    Model <: DocumentModel[Doc],
    Tx,
    S <: Store[Doc, Model] { type TX = Tx },
    Return
  ](list: List[S])(f: List[Tx] => Task[Return]): Task[Return] = {
    // Each acquired transaction is bracketed immediately. A later acquisition/body failure aborts
    // all open scopes. Successful commits across independent stores are NOT a distributed transaction.
    def loop(rest: List[S], acquired: List[Tx]): Task[Return] = rest match {
      case Nil => Task.defer(f(acquired.reverse))
      case s :: tail => s.transaction(tx => loop(tail, tx :: acquired))
    }
    loop(list, Nil)
  }

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    Return
  ](s1: S1, s2: S2)
   (f: (s1.TX, s2.TX) => Task[Return]): Task[Return] =
    s1.transaction { t1 =>
      s2.transaction { t2 =>
        Task.defer(f(t1, t2))
      }
    }

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    D3 <: Document[D3], M3 <: DocumentModel[D3], S3 <: Store[D3, M3],
    Return
  ](s1: S1, s2: S2, s3: S3)
   (f: (s1.TX, s2.TX, s3.TX) => Task[Return]): Task[Return] =
    s1.transaction { t1 =>
      s2.transaction { t2 =>
        s3.transaction { t3 =>
          Task.defer(f(t1, t2, t3))
        }
      }
    }

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    D3 <: Document[D3], M3 <: DocumentModel[D3], S3 <: Store[D3, M3],
    D4 <: Document[D4], M4 <: DocumentModel[D4], S4 <: Store[D4, M4],
    Return
  ](s1: S1, s2: S2, s3: S3, s4: S4)
   (f: (s1.TX, s2.TX, s3.TX, s4.TX) => Task[Return]): Task[Return] =
    s1.transaction { t1 =>
      s2.transaction { t2 =>
        s3.transaction { t3 =>
          s4.transaction { t4 =>
            Task.defer(f(t1, t2, t3, t4))
          }
        }
      }
    }

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    D3 <: Document[D3], M3 <: DocumentModel[D3], S3 <: Store[D3, M3],
    D4 <: Document[D4], M4 <: DocumentModel[D4], S4 <: Store[D4, M4],
    D5 <: Document[D5], M5 <: DocumentModel[D5], S5 <: Store[D5, M5],
    Return
  ](s1: S1, s2: S2, s3: S3, s4: S4, s5: S5)
   (f: (s1.TX, s2.TX, s3.TX, s4.TX, s5.TX) => Task[Return]): Task[Return] =
    s1.transaction { t1 =>
      s2.transaction { t2 =>
        s3.transaction { t3 =>
          s4.transaction { t4 =>
            s5.transaction { t5 =>
              Task.defer(f(t1, t2, t3, t4, t5))
            }
          }
        }
      }
    }

  def apply[
    D1 <: Document[D1], M1 <: DocumentModel[D1], S1 <: Store[D1, M1],
    D2 <: Document[D2], M2 <: DocumentModel[D2], S2 <: Store[D2, M2],
    D3 <: Document[D3], M3 <: DocumentModel[D3], S3 <: Store[D3, M3],
    D4 <: Document[D4], M4 <: DocumentModel[D4], S4 <: Store[D4, M4],
    D5 <: Document[D5], M5 <: DocumentModel[D5], S5 <: Store[D5, M5],
    D6 <: Document[D6], M6 <: DocumentModel[D6], S6 <: Store[D6, M6],
    Return
  ](s1: S1, s2: S2, s3: S3, s4: S4, s5: S5, s6: S6)
   (f: (s1.TX, s2.TX, s3.TX, s4.TX, s5.TX, s6.TX) => Task[Return]): Task[Return] =
    s1.transaction { t1 =>
      s2.transaction { t2 =>
        s3.transaction { t3 =>
          s4.transaction { t4 =>
            s5.transaction { t5 =>
              s6.transaction { t6 =>
                Task.defer(f(t1, t2, t3, t4, t5, t6))
              }
            }
          }
        }
      }
    }

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
   (f: (s1.TX, s2.TX, s3.TX, s4.TX, s5.TX, s6.TX, s7.TX) => Task[Return]): Task[Return] =
    s1.transaction { t1 =>
      s2.transaction { t2 =>
        s3.transaction { t3 =>
          s4.transaction { t4 =>
            s5.transaction { t5 =>
              s6.transaction { t6 =>
                s7.transaction { t7 =>
                  Task.defer(f(t1, t2, t3, t4, t5, t6, t7))
                }
              }
            }
          }
        }
      }
    }

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
   (f: (s1.TX, s2.TX, s3.TX, s4.TX, s5.TX, s6.TX, s7.TX, s8.TX) => Task[Return]): Task[Return] =
    s1.transaction { t1 =>
      s2.transaction { t2 =>
        s3.transaction { t3 =>
          s4.transaction { t4 =>
            s5.transaction { t5 =>
              s6.transaction { t6 =>
                s7.transaction { t7 =>
                  s8.transaction { t8 =>
                    Task.defer(f(t1, t2, t3, t4, t5, t6, t7, t8))
                  }
                }
              }
            }
          }
        }
      }
    }

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
   (f: (s1.TX, s2.TX, s3.TX, s4.TX, s5.TX, s6.TX, s7.TX, s8.TX, s9.TX) => Task[Return]): Task[Return] =
    s1.transaction { t1 =>
      s2.transaction { t2 =>
        s3.transaction { t3 =>
          s4.transaction { t4 =>
            s5.transaction { t5 =>
              s6.transaction { t6 =>
                s7.transaction { t7 =>
                  s8.transaction { t8 =>
                    s9.transaction { t9 =>
                      Task.defer(f(t1, t2, t3, t4, t5, t6, t7, t8, t9))
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

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
   (f: (s1.TX, s2.TX, s3.TX, s4.TX, s5.TX, s6.TX, s7.TX, s8.TX, s9.TX, s10.TX) => Task[Return]): Task[Return] =
    s1.transaction { t1 =>
      s2.transaction { t2 =>
        s3.transaction { t3 =>
          s4.transaction { t4 =>
            s5.transaction { t5 =>
              s6.transaction { t6 =>
                s7.transaction { t7 =>
                  s8.transaction { t8 =>
                    s9.transaction { t9 =>
                      s10.transaction { t10 =>
                        Task.defer(f(t1, t2, t3, t4, t5, t6, t7, t8, t9, t10))
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
}
