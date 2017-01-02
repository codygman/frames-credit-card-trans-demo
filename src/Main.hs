{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE PartialTypeSignatures #-}
module Main where

import Frames
import Frames.CSV
import Pipes hiding (Proxy)
import qualified Pipes.Prelude as P
import Frames.Time.LocalTime.Columns
-- import Frames.Time.LocalTime.TimeIn
import qualified Frames.Time.Chicago.Columns as C
import Data.Time
import Control.Lens
import Frames.Diff
import Data.Time.Lens
import Control.Monad.ST
import qualified Pipes as P
import Frames.InCore (RecVec)
import qualified Data.Foldable as F
import Rapid


-- TODO can remove the monad wrapped FrameRec with runST I think, like: https://github.com/acowley/Frames/blob/122636432ab425f4cbf12fd400996eab78ef1462/src/Frames/InCore.hs#L215
{-
-- | Keep only those rows of a 'FrameRec' that satisfy a predicate.
filterFrame :: RecVec rs => (Record rs -> Bool) -> FrameRec rs -> FrameRec rs
filterFrame p f = runST $ inCoreAoS $ P.each f P.>-> P.filter p
{-# INLINE filterFrame #-}
-}
-- tableTypes' rowGen { rowTypeName = "Transaction"
--                    , columnUniverse = $(colQ ''MyColumns)
--                    }
--   "data/purchasing-card-data-2014.csv"

tableTypes' rowGen { rowTypeName = "Transaction"
                   , columnUniverse = $(colQ ''C.MyColumns)
                   }
  "data/purchasing-card-data-2014.csv"

transactions :: Producer Transaction IO ()
transactions = readTable  "data/purchasing-card-data-2014.csv"

transactions100K :: Producer Transaction IO ()
transactions100K = readTable  "data/purchasing-card-data-2014-100K.csv"


transactions' :: Producer Transaction IO ()
transactions' = readTable  "data/purchasing-card-data-2014-medium.csv"

transactions'Maybe :: Producer (ColFun Maybe Transaction) IO ()
transactions'Maybe = readTableMaybeOpt transactionParser "data/purchasing-card-data-2014-medium.csv"



transactions'' :: Producer Transaction IO ()
transactions'' = readTable  "data/purchasing-card-data-2014-large.csv"


-- helpers
chicagoToZoned = (\(Chicago (TimeIn zt)) -> zt)


-- then we can answer something like: How many transactions happened in the month of April
-- λ> P.length (transactions >-> dateBetween transactionDate (dt 2014 4 1) (dt 2014 4 30))
-- 314

-- using pipePreview
-- λ> pipePreview transactions 1 (dateBetween transactionDate(d 2014 4 1) (d 2014 4 30))
-- {Service Area :-> "Childrens Services", Account Description :-> "IT Services", Creditor :-> "123-REG.CO.UK", Transaction Date :-> Chicago (TimeIn 2014-04-23 00:00:00 CDT), JV Reference :-> 93, JV Date :-> Chicago (TimeIn 2014-05-20 00:00:00 CDT), JV Value :-> 143.81}


-- same as above for a Frame
-- λ> transactions <- dateBetween' transactionDate (inCoreAoS transactions) (dt 2014 4 1) (dt 2014 4 30)
-- λ> print $ F.length transactions
-- 314
-- or as a one-liner
-- λ> print =<< F.length <$> dateBetween' transactionDate (inCoreAoS transactions) (dt 2014 4 1) (dt 2014 4 30)
-- 314


-- NOTE results are useless, only for purposes of perf testing join on columns that would create a lot of matches
-- selfJoinTest = do
--   joined <- innerJoin transactions transactionDate transactions transactionDate
--   mapM_ (print . frameRow joined) [0..10]


-- filtering a frame directly
-- mapM_ print $ take 5 . F.toList $ filterFrame (\r -> rget accountDescription r == "IT Services") ts
-- same as above but using takeFrame
-- mapM_ print $ takeFrame 5 $ filterFrame (\r -> rget accountDescription r == "IT Services") ts

r = do
  rapid 0 $ \r -> do
    transactionsFrame <- createRef r "transactionsFrame" $
      inCoreAoS transactions'
    print $ F.length transactionsFrame

-- initial load
-- λ> r
-- 996045
-- (111.62 secs, 215,025,059,952 bytes)

-- using rapid by keeping it in memory
-- λ> r
-- 996045
-- (0.10 secs, 232,068,480 bytes)
-- λ> -- using 3208 mb of memory with medium file in memory

-- compiled with -O1
-- cody@zentop:~/source/frames-credit-card-trans-demo$ time .stack-work/dist/x86_64-linux/Cabal-1.24.0.0/build/frames-credit-card-trans-demo/frames-credit-card-trans-demo
-- 996045

-- real    0m52.198s
-- user    0m51.944s
-- sys     0m0.228s

-- compiled with -O1 -threaded
-- cody@zentop:~/source/frames-credit-card-trans-demo$ time .stack-work/dist/x86_64-linux/Cabal-1.24.0.0/build/frames-credit-card-trans-demo/frames-credit-card-trans-demo
-- 996045

-- real    0m53.573s
-- user    0m53.308s
-- sys     0m0.264s



-- testing peformance with 100K rows
main :: IO ()
main = do
  P.length transactions100K >>= print


-- stack install --ghc-options="-O2 -rtsopts"
-- cody@zentop:~/source/frames-credit-card-trans-demo$ time frames-credit-card-trans-demo +RTS -s
-- 99604
--   18,574,698,792 bytes allocated in the heap
--      123,360,848 bytes copied during GC
--          108,048 bytes maximum residency (3 sample(s))
--           62,632 bytes maximum slop
--                4 MB total memory in use (0 MB lost due to fragmentation)

--                                      Tot time (elapsed)  Avg pause  Max pause
--   Gen  0     35876 colls, 35876 par    1.436s   0.552s     0.0000s    0.0142s
--   Gen  1         3 colls,     2 par    0.004s   0.002s     0.0007s    0.0018s

--   Parallel GC work balance: 0.03% (serial 0%, perfect 100%)

--   TASKS: 10 (1 bound, 9 peak workers (9 total), using -N4)

--   SPARKS: 0 (0 converted, 0 overflowed, 0 dud, 0 GC'd, 0 fizzled)

--   INIT    time    0.000s  (  0.002s elapsed)
--   MUT     time   10.136s  (  7.182s elapsed)
--   GC      time    1.440s  (  0.554s elapsed)
--   EXIT    time    0.004s  (  0.006s elapsed)
--   Total   time   11.580s  (  7.744s elapsed)

--   Alloc rate    1,832,547,236 bytes per MUT second

--   Productivity  87.6% of total user, 130.9% of total elapsed

-- gc_alloc_block_sync: 14932
-- whitehole_spin: 0
-- gen[0].sync: 0
-- gen[1].sync: 0

-- real    0m7.748s
-- user    0m11.580s
-- sys     0m2.396s

-- stack install --ghc-options="-O2 -rtsopts -threaded -with-rtsopts=-N"
-- cody@zentop:~/source/frames-credit-card-trans-demo$ time frames-credit-card-trans-demo +RTS -s
-- 99604
--   18,574,698,792 bytes allocated in the heap
--      123,360,848 bytes copied during GC
--          108,048 bytes maximum residency (3 sample(s))
--           55,120 bytes maximum slop
--                4 MB total memory in use (0 MB lost due to fragmentation)

--                                      Tot time (elapsed)  Avg pause  Max pause
--   Gen  0     35876 colls, 35876 par    1.360s   0.425s     0.0000s    0.0113s
--   Gen  1         3 colls,     2 par    0.000s   0.001s     0.0003s    0.0008s

--   Parallel GC work balance: 0.05% (serial 0%, perfect 100%)

--   TASKS: 10 (1 bound, 9 peak workers (9 total), using -N4)

--   SPARKS: 0 (0 converted, 0 overflowed, 0 dud, 0 GC'd, 0 fizzled)

--   INIT    time    0.004s  (  0.002s elapsed)
--   MUT     time    9.632s  (  6.998s elapsed)
--   GC      time    1.360s  (  0.426s elapsed)
--   EXIT    time    0.000s  (  0.005s elapsed)
--   Total   time   10.996s  (  7.431s elapsed)

--   Alloc rate    1,928,436,336 bytes per MUT second

--   Productivity  87.6% of total user, 129.6% of total elapsed

-- gc_alloc_block_sync: 37266
-- whitehole_spin: 0
-- gen[0].sync: 0
-- gen[1].sync: 0

-- real    0m7.436s
-- user    0m10.996s
-- sys     0m2.180s

-- same but with -N4 explicitly set
-- cody@zentop:~/source/frames-credit-card-trans-demo$ time frames-credit-card-trans-demo +RTS -s -N4
-- 99604
--   18,574,698,792 bytes allocated in the heap
--      123,360,624 bytes copied during GC
--          107,808 bytes maximum residency (3 sample(s))
--           56,032 bytes maximum slop
--                4 MB total memory in use (0 MB lost due to fragmentation)

--                                      Tot time (elapsed)  Avg pause  Max pause
--   Gen  0     35876 colls, 35876 par    1.072s   0.421s     0.0000s    0.0055s
--   Gen  1         3 colls,     2 par    0.004s   0.000s     0.0001s    0.0003s

--   Parallel GC work balance: 0.04% (serial 0%, perfect 100%)

--   TASKS: 10 (1 bound, 9 peak workers (9 total), using -N4)

--   SPARKS: 0 (0 converted, 0 overflowed, 0 dud, 0 GC'd, 0 fizzled)

--   INIT    time    0.000s  (  0.001s elapsed)
--   MUT     time    9.648s  (  6.898s elapsed)
--   GC      time    1.076s  (  0.422s elapsed)
--   EXIT    time    0.000s  (  0.004s elapsed)
--   Total   time   10.724s  (  7.325s elapsed)

--   Alloc rate    1,925,238,266 bytes per MUT second

--   Productivity  90.0% of total user, 131.7% of total elapsed

-- gc_alloc_block_sync: 30968
-- whitehole_spin: 0
-- gen[0].sync: 0
-- gen[1].sync: 0

-- real    0m7.327s
-- user    0m10.724s
-- sys     0m2.232s

-- same as above but with -maxN4
-- cody@zentop:~/source/frames-credit-card-trans-demo$ time frames-credit-card-trans-demo +RTS -s -maxN4
-- 99604
--   18,574,698,776 bytes allocated in the heap
--      123,326,688 bytes copied during GC
--          108,192 bytes maximum residency (3 sample(s))
--           51,552 bytes maximum slop
--                4 MB total memory in use (0 MB lost due to fragmentation)

--                                      Tot time (elapsed)  Avg pause  Max pause
--   Gen  0     35876 colls, 35876 par    1.288s   0.505s     0.0000s    0.0167s
--   Gen  1         3 colls,     2 par    0.004s   0.001s     0.0002s    0.0004s

--   Parallel GC work balance: 0.05% (serial 0%, perfect 100%)

--   TASKS: 10 (1 bound, 9 peak workers (9 total), using -N4)

--   SPARKS: 0 (0 converted, 0 overflowed, 0 dud, 0 GC'd, 0 fizzled)

--   INIT    time    0.000s  (  0.001s elapsed)
--   MUT     time   10.016s  (  7.031s elapsed)
--   GC      time    1.292s  (  0.506s elapsed)
--   EXIT    time    0.004s  (  0.006s elapsed)
--   Total   time   11.312s  (  7.544s elapsed)

--   Alloc rate    1,854,502,673 bytes per MUT second

--   Productivity  88.6% of total user, 132.8% of total elapsed

-- gc_alloc_block_sync: 42106
-- whitehole_spin: 0
-- gen[0].sync: 0
-- gen[1].sync: 0

-- real    0m7.548s
-- user    0m11.312s
-- sys     0m2.208s


-- same as fastest so far, but with -A256M
-- cody@zentop:~/source/frames-credit-card-trans-demo$ time frames-credit-card-trans-demo +RTS -s -N4 -A256M
-- 99604
--   18,574,698,792 bytes allocated in the heap
--          348,560 bytes copied during GC
--          108,648 bytes maximum residency (2 sample(s))
--           51,096 bytes maximum slop
--             1041 MB total memory in use (0 MB lost due to fragmentation)

--                                      Tot time (elapsed)  Avg pause  Max pause
--   Gen  0        68 colls,    68 par    0.008s   0.002s     0.0000s    0.0004s
--   Gen  1         2 colls,     1 par    0.000s   0.000s     0.0002s    0.0002s

--   Parallel GC work balance: 13.76% (serial 0%, perfect 100%)

--   TASKS: 10 (1 bound, 9 peak workers (9 total), using -N4)

--   SPARKS: 0 (0 converted, 0 overflowed, 0 dud, 0 GC'd, 0 fizzled)

--   INIT    time    0.004s  (  0.007s elapsed)
--   MUT     time    5.216s  (  5.252s elapsed)
--   GC      time    0.008s  (  0.002s elapsed)
--   EXIT    time    0.000s  (  0.000s elapsed)
--   Total   time    5.228s  (  5.262s elapsed)

--   Alloc rate    3,561,100,228 bytes per MUT second

--   Productivity  99.8% of total user, 99.1% of total elapsed

-- gc_alloc_block_sync: 36
-- whitehole_spin: 0
-- gen[0].sync: 0
-- gen[1].sync: 1

-- real    0m5.277s
-- user    0m5.228s
-- sys     0m0.064s

-- stack install --executable-profiling --library-profiling --ghc-options="-O2 -fprof-auto -rtsopts -threaded -with-rtsopts=-N"
-- cody@zentop:~/source/frames-credit-card-trans-demo$ frames-credit-card-trans-demo +RTS -s -N4 -A256M
-- 99604
--   31,312,378,864 bytes allocated in the heap
--          732,648 bytes copied during GC
--          135,936 bytes maximum residency (2 sample(s))
--           50,960 bytes maximum slop
--             1041 MB total memory in use (0 MB lost due to fragmentation)

--                                      Tot time (elapsed)  Avg pause  Max pause
--   Gen  0       117 colls,   117 par    0.016s   0.008s     0.0001s    0.0051s
--   Gen  1         2 colls,     1 par    0.000s   0.000s     0.0002s    0.0003s

--   Parallel GC work balance: 6.37% (serial 0%, perfect 100%)

--   TASKS: 10 (1 bound, 9 peak workers (9 total), using -N4)

--   SPARKS: 0 (0 converted, 0 overflowed, 0 dud, 0 GC'd, 0 fizzled)

--   INIT    time    0.016s  (  0.021s elapsed)
--   MUT     time    9.492s  (  9.534s elapsed)
--   GC      time    0.016s  (  0.008s elapsed)
--   RP      time    0.000s  (  0.000s elapsed)
--   PROF    time    0.000s  (  0.000s elapsed)
--   EXIT    time    0.000s  (  0.000s elapsed)
--   Total   time    9.528s  (  9.563s elapsed)

--   Alloc rate    3,298,817,832 bytes per MUT second

--   Productivity  99.7% of total user, 99.3% of total elapsed

-- gc_alloc_block_sync: 46
-- whitehole_spin: 0
-- gen[0].sync: 0
-- gen[1].sync: 0

-- ========================================================================
-- frames-credit-card-trans-demo +RTS -s -N4 -A256M
-- comparing profs:

-- before using Data.Thyme.Time:
-- Mon Jan  2 01:55 2017 Time and Allocation Profiling Report  (Final)

-- 	   frames-credit-card-trans-demo +RTS -N -p -N4 -A256M -RTS

-- 	total time  =       22.00 secs   (88003 ticks @ 1000 us, 4 processors)
-- 	total alloc = 141,281,829,568 bytes  (excludes profiling overheads)

-- COST CENTRE                               MODULE                          %time %alloc

-- >>=.\.succ'                               Data.Attoparsec.Internal.Types   23.1    4.3
-- >>=.\                                     Data.Attoparsec.Internal.Types   17.8    7.1
-- parseLocalTime.mkParser                   Frames.Time.LocalTime.TimeIn     15.0   54.2


-- after using Data.Thyme.Time:
-- Mon Jan  2 01:46 2017 Time and Allocation Profiling Report  (Final)

-- 	   frames-credit-card-trans-demo +RTS -N -p -N4 -A256M -RTS

-- 	total time  =       21.79 secs   (87171 ticks @ 1000 us, 4 processors)
-- 	total alloc = 141,281,829,056 bytes  (excludes profiling overheads)

-- COST CENTRE                               MODULE                          %time %alloc

-- >>=.\.succ'                               Data.Attoparsec.Internal.Types   21.4    4.3
-- >>=.\                                     Data.Attoparsec.Internal.Types   18.5    7.1
-- parseLocalTime.mkParser                   Frames.Time.LocalTime.TimeIn     15.3   54.2




-- stack install --executable-profiling --library-profiling --ghc-options="-O2 -fprof-auto -rtsopts -threaded -with-rtsopts=-N"
-- stack install --executable-profiling --library-profiling --ghc-options="-O2 -fprof-auto -rtsopts -threaded -with-rtsopts=-N"
-- stack install --executable-profiling --library-profiling --ghc-options="-O2 -fprof-auto -rtsopts -threaded -with-rtsopts=-N"


-- main :: IO ()
-- main = do
--   -- runEffect $ transactions >-> transactionDateBetween (fromGregorian 2014 4 1) (fromGregorian 2014 4 2) >-> P.take 5 >-> P.print
--   -- runEffect $ transactions >-> transactionDateBetween (fromGregorian 2014 4 2) (fromGregorian 2014 4 3)  >-> P.take 5 >-> P.print
--   -- runEffect $ transactions >-> transactionDate `withinPastNDays` 30 >-> P.take 5 >-> P.print

--   -- using dateBetween' on a frame
--   -- transactions <- dateBetween' transactionDate (inCoreAoS transactions) (dt 2014 4 1) (dt 2014 4 6)
--   -- print $ F.length transactions
--   P.length transactions' >>= print
--   -- print =<< F.length <$> dateBetween' transactionDate (inCoreAoS transactions') (dt 2014 4 1) (dt 2014 4 30)


--   -- putStrLn "hello world"


-- -- P.length transactions' (ghci using Data.Time)
-- -- λ> P.length transactions'
-- -- 996045
-- -- (97.30 secs, 209,066,524,504 bytes)

-- -- P.length transactions' (ghci using Data.Thyme)
-- -- λ> P.length transactions'
-- -- 999999
-- -- (32.84 secs, 33,111,184,072 bytes)

-- -- P.length transactions' (compiled) (I think the ghc-options were: -O1
-- -- real    0m53.712s (+ 20 seconds of switch to shell/compile time)

-- -- P.length transactions' (compiled) (no threaded)
-- -- stack install --executable-profiling --library-profiling --ghc-options="-O1 -fprof-auto"
-- -- cody@zentop:~/source/frames-credit-card-trans-demo$ time frames-credit-card-trans-demo 
-- -- 996045

-- -- real    1m38.319s
-- -- user    1m37.744s
-- -- sys     0m0.492s


-- -- P.length transactions' (compiled)
-- -- stack install --executable-profiling --library-profiling --ghc-options="-O1 -fprof-auto -rtsopts -threaded -with-rtsopts=-N"
-- -- cody@zentop:~/source/frames-credit-card-trans-demo$ time frames-credit-card-trans-demo
-- -- 996045

-- -- real    1m38.142s
-- -- user    1m37.672s
-- -- sys     0m0.456s


-- -- P.length transactions' (compiled)
-- -- stack install --executable-profiling --library-profiling --ghc-options="-O2 -fprof-auto -rtsopts -threaded -with-rtsopts=-N"
-- -- run
-- -- cody@zentop:~/source/frames-credit-card-trans-demo$ time frames-credit-card-trans-demo
-- -- 996045

-- -- real    1m36.420s
-- -- user    1m35.948s
-- -- sys     0m0.456s

-- -- cody@zentop:~/source/frames-credit-card-trans-demo$ time frames-credit-card-trans-demo +RTS -N4
-- -- 996045

-- -- real    2m12.176s
-- -- user    3m18.540s
-- -- sys     0m37.604s

-- -- cody@zentop:~/source/frames-credit-card-trans-demo$ time frames-credit-card-trans-demo +RTS -maxN4



-- -- finding out why there are only 996045 records
-- -- sample record:
-- -- {Just Service Area :-> "Childrens Services", Just Account Description :-> "Other Services", Just Creditor :-> "ACCESS EXPEDITIONS", Just Transaction Date :-> Chicago (TimeIn 2014-04-03 00:00:00 CDT), Just JV Reference :-> 111, Just JV Date :-> Chicago (TimeIn 2014-05-20 00:00:00 CDT), Nothing}
-- -- final record is Nothing.. what is final record
-- -- λ> :i Transaction
-- -- type Transaction =
-- --   Record
-- --     '["Service Area" :-> Text, "Account Description" :-> Text,
-- --       "Creditor" :-> Text, "Transaction Date" :-> Chicago,
-- --       "JV Reference" :-> Int, "JV Date" :-> Chicago,
-- --       "JV Value" :-> Double]

-- -- final record is a double, what is the value that is there? (almost certainly empty!)


-- -- testing dateBetween' on 4544 records BEFORE adding INLINE pragma
-- -- λ> print =<< F.length <$> dateBetween' transactionDate (inCoreAoS transactions) (dt 2014 4 1) (dt 2014 4 30)
-- -- 314
-- -- (1.65 secs, 5,463,166,968 bytes)

-- -- testing dateBetween' on 4544 records AFTER adding INLINE pragma
-- -- 314
-- -- (1.65 secs, 5,463,166,896 bytes)


-- -- testing dateBetween' on 4544 records BEFORE adding INLINE pragma (compiled)
-- -- 0m0.308s

-- -- testing dateBetween' on 4544 records AFTER adding INLINE pragma (compiled)
-- -- 0m0.283s


-- -- testing dateBetween on (1^6)-1 records BEFORE adding INLINE pragma (compiled)

-- -- testing dateBetween on (1^6)-1 records AFTER adding INLINE pragma (compiled)
