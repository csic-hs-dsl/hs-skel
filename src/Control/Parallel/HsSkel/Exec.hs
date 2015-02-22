{-# LANGUAGE GADTs #-}
{-# LANGUAGE Arrows #-}

module Control.Parallel.HsSkel.Exec where

import Control.Parallel.HsSkel.DSL

import Data.Foldable (mapM_)
import Data.Traversable (mapM)
import Data.Vector (freeze)
import Data.Vector.Mutable (new, write, take)
import Control.Category ((.))
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM (readTBQueue, atomically, writeTBQueue, newTBQueueIO, TBQueue)
import Control.DeepSeq (NFData, rnf)
import Control.Exception (evaluate)
import Control.Monad (liftM)
import Prelude hiding (id, mapM, mapM_, take, (.))


--import Data.Time.Clock (getCurrentTime, diffUTCTime)

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
                    _ <- eval v
                    atomically $ writeTBQueue q (Just v)
                    execGen q i'
                Nothing -> atomically $ writeTBQueue q Nothing

--execStream (StMap (StMap stream' cons') cons) =  execStream (StMap stream' (cons . cons'))

execStream (StMap cons stream) = do
    qo <- newTBQueueIO queueLimit
    qi <- execStream stream
    _ <- forkIO $ consumer qi qo
    return qo
    where 
        consumer qi qo = do
            res <- atomically $ readTBQueue qi
            case res of 
                Just vi -> do 
                    vo <- exec cons vi
                    atomically $ writeTBQueue qo (Just vo)
                    consumer qi qo
                Nothing -> atomically $ writeTBQueue qo Nothing
execStream (StChunk chunkSize stream) = do
    qo <- newTBQueueIO queueLimit
    qi <- execStream stream
    storage <- new chunkSize
    _ <- forkIO $ recc qi qo storage 0
    return qo
    where 
        recc qi qo storage pos = do
            i <- atomically $ readTBQueue qi
            case i of
                Just vi -> do
                    write storage pos vi
                    let newCant = pos + 1
                    if (newCant == chunkSize) 
                        then do
                            vector <- freeze storage
                            atomically $ writeTBQueue qo (Just vector)
                            recc qi qo storage 0
                        else do
                            recc qi qo storage newCant
                Nothing -> do
                    if (pos > 0)
                        then do
                            vector <- freeze $ take pos storage
                            atomically $ writeTBQueue qo (Just vector)
                        else
                            return ()
                    atomically $ writeTBQueue qo Nothing
execStream (StUnChunk stream) = do
    qo <- newTBQueueIO queueLimit
    qi <- execStream stream
    _ <- forkIO $ recc qi qo
    return qo
    where 
        recc qi qo = do
            i <- atomically $ readTBQueue qi
            case i of
                Just vsi -> do
                    mapM_ (\v -> atomically $ writeTBQueue qo (Just v)) vsi
                    recc qi qo
                Nothing -> do
                    atomically $ writeTBQueue qo Nothing

{- ================================================================== -}
{- ======================== Skel Execution ========================== -}
{- ================================================================== -}

exec :: Skel i o -> i -> IO o
exec (SkSeq f) = (eval =<<) . liftM f . return
exec (SkSeq_ f) = liftM f . return
exec (SkPar sk) = \i -> (do
    mVar <- newEmptyMVar
    _ <- forkIO (stuff i mVar)
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
exec (SkRed red stream) = \z -> do
    q <- execStream stream   
    reducer q z
    where 
        reducer q z = do
            res <- atomically $ readTBQueue q
            case res of
                Just v -> do
                    z' <- exec red (z, v)
                    reducer q z'
                Nothing -> do 
                    return z

