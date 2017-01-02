{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE PartialTypeSignatures #-}
import Criterion.Main

import Frames
import Frames.CSV
import Frames.Diff
import Frames.Time.LocalTime.Columns
import qualified Frames.Time.Chicago.Columns as C
import qualified Pipes as P
import qualified Pipes.Prelude as P
import Pipes hiding (Proxy)

tableTypes' rowGen { rowTypeName = "TransactionTime"
                   , columnUniverse = $(colQ ''C.MyColumns)
                   }
  "data/purchasing-card-data-2014-large.csv"

tableTypes' rowGen { rowTypeName = "TransactionThyme"
                   , columnUniverse = $(colQ ''C.MyColumns)
                   }
  "data/purchasing-card-data-2014-large.csv"

transactionsTime :: Producer TransactionTime IO ()
transactionsTime = readTable  "data/purchasing-card-data-2014-large.csv"

transactionsThyme :: Producer TransactionThyme IO ()
transactionsThyme = readTable  "data/purchasing-card-data-2014-large.csv"


-- Our benchmark harness.
main = defaultMain [ bgroup "length" [ bench "time 5K"  (nfIO (P.length (transactionsTime P.>-> P.take 5000)))
                                     -- , bench "thyme 5K"  $ P.length (transactionsThyme P.>-> P.take 5000)
                                     -- , bench "time 50K"  $ nf P.length (transactionsTime P.>-> P.take 50000)
                                     -- , bench "thyme 50K"  $ nf P.length (transactionsThyme P.>-> P.take 50000)
                                     ]
                   , bgroup "datebetween" [ bench "datebetween on 5000 time" $ nfIO $ P.length (transactionsTime P.>-> P.take 5000 P.>-> dateBetween transactionDate(dt 2014 4 1) (dt 2014 4 30))
                                          ]
  ]
