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
dateBetween :: _
            -> Day
            -> Day
            -> Pipe (Record rs) (Record rs) IO r
dateBetween target start end = P.filter go
  where go :: Record rs -> _
        go r = let targetDate = rget target r :: Chicago
                   targetDate' = chicagoToZoned targetDate :: ZonedTime
                   targetDay = localDay (zonedTimeToLocalTime targetDate') :: Day
               in
                 targetDay >= start && targetDay < end
-- type error
-- src/Main.hs:48:38: error: …
--     • Couldn't match expected type ‘(Chicago -> f Chicago)
--                                     -> Record rs1 -> f (Record rs1)’
--                   with actual type ‘t’
--         because type variable ‘f’ would escape its scope
--       This (rigid, skolem) type variable is bound by
--         a type expected by the context:
--           Functor f => (Chicago -> f Chicago) -> Record rs1 -> f (Record rs1)
--         at /home/cody/source/frames-credit-card-trans-demo/src/Main.hs:48:33-45
--     • In the first argument of ‘rget’, namely ‘target’
--       In the expression: rget target r :: Chicago
--       In an equation for ‘targetDate’:
--           targetDate = rget target r :: Chicago
--     • Relevant bindings include
--         r :: Record rs1
--           (bound at /home/cody/source/frames-credit-card-trans-demo/src/Main.hs:48:12)
--         go :: Record rs1 -> Bool
--           (bound at /home/cody/source/frames-credit-card-trans-demo/src/Main.hs:48:9)
--         target :: t
--           (bound at /home/cody/source/frames-credit-card-trans-demo/src/Main.hs:46:13)
--         dateBetween :: t -> Day -> Day -> Pipe (Record rs) (Record rs) IO r
--           (bound at /home/cody/source/frames-credit-card-trans-demo/src/Main.hs:46:1)
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
