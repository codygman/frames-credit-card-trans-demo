{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}
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
import Control.Concurrent (threadDelay)

import Control.DeepSeq.Generics (genericRnf)
import Control.DeepSeq
import GHC.Generics

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
main = defaultMain [ bgroup "with time"
                       [ bench "length 5K"  (nfIO (P.length (transactionsTime P.>-> P.take 5000)))
                       , bench "length 100K"  (nfIO (P.length (transactionsTime P.>-> P.take 100000)))

                       , bench "datebetween 5K" $ nfIO $ P.length (
                           transactionsTime P.>-> P.take 5000 P.>-> dateBetween transactionDate(dt 2014 4 1) (dt 2014 4 30)
                           )

                       -- , bench "datebetween on 100000" $ nfIO $ P.length (
                       --     transactionsTime P.>-> P.take 100000 P.>-> dateBetween transactionDate(dt 2014 4 1) (dt 2014 4 30)
                       --     )

                       -- TODO requires NFData instance for Frames/FrameRec
                       -- , bench "innerjoin on 5000" $ nfIO $
                       --     innerJoin
                       --      (transactionsTime P.>-> P.take 5000)
                       --      creditor
                       --      (transactionsTime P.>-> P.take 5000)
                       --      creditor
                       -- ]

                   , bgroup "with thyme"
                       [ bench "length 5K"  (nfIO (P.length (transactionsThyme P.>-> P.take 5000)))
                       , bench "length 100K"  (nfIO (P.length (transactionsThyme P.>-> P.take 100000)))

                       , bench "datebetween 5k" $ nfIO $ P.length (
                           transactionsThyme P.>-> P.take 5000 P.>-> dateBetween transactionDate(dt 2014 4 1) (dt 2014 4 30)
                           )

                       -- , bench "datebetween on 100000" $ nfIO $ P.length (
                       --     transactionsThyme P.>-> P.take 100000 P.>-> dateBetween transactionDate(dt 2014 4 1) (dt 2014 4 30)
                       --     )


                       ]
                   ]
