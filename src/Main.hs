{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TemplateHaskell #-}
module Main where

import Frames
import Frames.CSV
import Pipes hiding (Proxy)
import qualified Pipes.Prelude as P
import Frames.Time.Chicago.Columns
import Frames.Time.Chicago.TimeIn
import Data.Time


tableTypes' rowGen { rowTypeName = "Transaction"
                   , columnUniverse = $(colQ ''MyColumns)
                   }
  "data/purchasing-card-data-2014.csv"

transactions :: Producer Transaction IO ()
transactions = readTable  "data/purchasing-card-data-2014.csv"

-- helpers
chicagoToZoned = (\(Chicago (TimeIn zt)) -> zt)
d = fromGregorian

-- between meaning on or after start but before end
transactionDateBetween :: (TransactionDate ∈ rs) =>
               Day
            -> Day
            -> Pipe (Record rs) (Record rs) IO r
transactionDateBetween start end = P.filter go
  where go r = let targetDate = rget transactionDate r -- :: Chicago
                   targetDate' = chicagoToZoned targetDate -- :: ZonedTime
                   targetDay = localDay (zonedTimeToLocalTime targetDate') :: Day
               in
                 targetDay >= start && targetDay < end


-- between meaning on or after start but before end
dateBetween :: forall (rs :: [*]) (m :: *) (f :: * -> *).
               (Functor f) =>
               ((Chicago -> f Chicago) -> Record rs -> f (Record rs))
            -> Day
            -> Day
            -> Pipe (Record rs) (Record rs) IO m
dateBetween target start end = P.filter go
  where go :: Record rs -> _
        go r = let targetDate = (rget target r) :: Chicago
                   targetDate' = chicagoToZoned targetDate :: ZonedTime
                   targetDay = localDay (zonedTimeToLocalTime targetDate') :: Day
               in
                 targetDay >= start && targetDay < end
-- type error
-- src/Main.hs:53:39: error: …
--     • Couldn't match type ‘f’ with ‘f1’
--       ‘f’ is a rigid type variable bound by
--         the type signature for:
--           dateBetween :: forall (rs :: [*]) m (f :: * -> *).
--                          Functor f =>
--                          ((Chicago -> f Chicago) -> Record rs -> f (Record rs))
--                          -> Day -> Day -> Pipe (Record rs) (Record rs) IO m
--         at /home/cody/source/frames-credit-card-trans-demo/src/Main.hs:45:45
--       ‘f1’ is a rigid type variable bound by
--         a type expected by the context:
--           forall (f1 :: * -> *).
--           Functor f1 =>
--           (Chicago -> f1 Chicago) -> Record rs -> f1 (Record rs)
--         at /home/cody/source/frames-credit-card-trans-demo/src/Main.hs:53:34
--       Expected type: (Chicago -> f1 Chicago)
--                      -> Record rs -> f1 (Record rs)
--         Actual type: (Chicago -> f Chicago) -> Record rs -> f (Record rs)
--     • In the first argument of ‘rget’, namely ‘target’
--       In the expression: (rget target r) :: Chicago
--       In an equation for ‘targetDate’:
--           targetDate = (rget target r) :: Chicago
--     • Relevant bindings include
--         target :: (Chicago -> f Chicago) -> Record rs -> f (Record rs)
--           (bound at /home/cody/source/frames-credit-card-trans-demo/src/Main.hs:51:13)
--         dateBetween :: ((Chicago -> f Chicago)
--                         -> Record rs -> f (Record rs))
--                        -> Day -> Day -> Pipe (Record rs) (Record rs) IO m
--           (bound at /home/cody/source/frames-credit-card-trans-demo/src/Main.hs:51:1)
-- Compilation failed.


-- then we can answer something like: How many transactions happened in the month of April
-- λ> P.length (transactions >-> transactionDateBetween (d 2014 4 1) (d 2014 4 30))
-- 314

-- using pipePreview
-- λ> pipePreview transactions 3 (transactionDateBetween (d 2014 4 1) (d 2014 4 30))
-- {Service Area :-> "Childrens Services", Account Description :-> "IT Services", Creditor :-> "123-REG.CO.UK", Transaction Date :-> Chicago (TimeIn 2014-04-23 00:00:00 CDT), JV Reference :-> 93, JV Date :-> Chicago (TimeIn 2014-05-20 00:00:00 CDT), JV Value :-> 143.81}
-- {Service Area :-> "Childrens Services", Account Description :-> "Equipment and Materials Repair", Creditor :-> "AFE SERVICELINE", Transaction Date :-> Chicago (TimeIn 2014-04-02 00:00:00 CDT), JV Reference :-> 6, JV Date :-> Chicago (TimeIn 2014-05-20 00:00:00 CDT), JV Value :-> 309.38}
-- {Service Area :-> "Childrens Services", Account Description :-> "Equipment and Materials Repair", Creditor :-> "AFE SERVICELINE", Transaction Date :-> Chicago (TimeIn 2014-04-02 00:00:00 CDT), JV Reference :-> 7, JV Date :-> Chicago (TimeIn 2014-05-20 00:00:00 CDT), JV Value :-> 218.76}



main :: IO ()
main = do
  -- runEffect $ transactions >-> transactionDateBetween (fromGregorian 2014 4 1) (fromGregorian 2014 4 2) >-> P.take 5 >-> P.print
  runEffect $ transactions >-> transactionDateBetween (fromGregorian 2014 4 2) (fromGregorian 2014 4 3)  >-> P.take 5 >-> P.print
  putStrLn "hello world"
