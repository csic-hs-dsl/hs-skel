{-# LANGUAGE GADTs #-}

module Control.Parallel.HsSkel.Exec where

import Control.Parallel.HsSkel

import Data.Traversable (mapM)
import Control.Category ((.))
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM (readTBQueue, atomically, writeTBQueue, newTBQueueIO, TBQueue)
import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate)
import Control.Monad hiding (mapM)
import Data.List (transpose)
import Prelude hiding (mapM, id, (.))

{- ================================================================== -}
{- =========================== Auxiliary ============================ -}
{- ================================================================== -}

queueLimit :: Int
queueLimit = 100000

eval :: (NFData a) => a -> IO a
eval a = do
    evaluate $ rnf a
    return a

{- ================================================================== -}
{- ======================= Stream Execution ========================= -}
{- ================================================================== -}

execStream :: Stream i -> IO (TBQueue (Maybe i))
execStream (StGen gen i) = do
    q <- newTBQueueIO queueLimit
    _ <- forkIO $ execGen q i
    return q
    where 
        execGen q i = do
            let res = gen i
            case res of 
                Just (v, i') -> do 
                    eval v
                    atomically $ writeTBQueue q (Just v)
                    execGen q i'
                Nothing -> atomically $ writeTBQueue q Nothing

--execStream (StMap (StMap stream' cons') cons) =  execStream (StMap stream' (cons . cons'))

execStream (StMap stream cons) = do
    qo <- newTBQueueIO queueLimit
    qi <- execStream stream
    _ <- forkIO $ consumer qi qo
    return qo
    where 
        consumer qi qo = do
            res <- atomically $ readTBQueue qi
            case res of 
                Just vi -> do 
                    --print ("StMap inicio ")-- ++ show vi)
                    vo <- exec cons vi
                    --print ("StMap fin ")-- ++ show vi)
                    atomically $ writeTBQueue qo (Just vo)
                    consumer qi qo
                Nothing -> atomically $ writeTBQueue qo Nothing
execStream (StChunk stream chunkSize) = do
    qo <- newTBQueueIO queueLimit
    qi <- execStream stream
    recc qi qo [] 0
    return qo
    where 
        recc qi qo ch cant = do
            i <- atomically $ readTBQueue qi
            case i of
                Just vi -> do
                    let newCh = vi:ch
                        newCant = cant + 1
                    if (newCant == chunkSize) 
                        then do
                            atomically $ writeTBQueue qo (Just $ reverse newCh)
                            recc qi qo [] 0
                        else do
                            recc qi qo newCh newCant
                Nothing -> do
                    atomically $ writeTBQueue qo (Just $ reverse ch)
                    atomically $ writeTBQueue qo Nothing

{- ================================================================== -}
{- ======================== Skel Execution ========================== -}
{- ================================================================== -}

exec :: Skel i o -> i -> IO o
exec (SkSeq f) = (eval =<<) . liftM f . return
exec (SkSeq_ f) = liftM f . return
exec (SkPar sk) = \i -> (do
    mVar <- newEmptyMVar
    forkIO (stuff i mVar)
    return $ Future mVar)
    where
        stuff i mVar = do
            --print ("SkPar inicio " ++ show i)
            r <- exec sk i
            --print ("SkPar fin " ++ show i)
            putMVar mVar r
exec (SkSync) = takeMVar . mvar
exec (SkComp s2 s1) = (exec s2 =<<) . exec s1
exec (SkPair sk1 sk2) = \(i1, i2) -> do
    o1 <- exec sk1 i1
    o2 <- exec sk2 i2
    return (o1, o2)
exec (SkMap s) = mapM (exec s)
exec (SkIf sl sr) = \input ->
    case input of
        Left i -> exec sl i >>= return . Left
        Right i -> exec sr i >>= return . Right
exec (SkApply) = \(sk, i) -> exec sk i
exec (SkRed stream red) = \z -> do
    q <- execStream stream   
    reducer q z
    where 
        reducer q z = do
            res <- atomically $ readTBQueue q
            case res of
                Just v -> do
                    z' <- exec red (z, v)
                    reducer q z'
                Nothing -> return z  

