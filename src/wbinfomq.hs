{-#LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Concurrent.HEP as HEP
import Control.Concurrent
import Control.Concurrent.HEP.Syslog
import System.IO
import Control.Monad.Trans
import Control.Monad.Trans.Either
import Control.Monad
import Network.AMQP as AMQP
import Data.Typeable
import System.Posix.Signals


data WBInfoMessage = WBSignalStop 
                   | WBSignalReload
    deriving Typeable
instance HEP.Message WBInfoMessage

main = runHEPGlobal $! procWithBracket wbInfoInit wbInfoShutdown $! proc wbInfoWorker

wbInfoInit :: HEPProc
wbInfoInit = do
    startSyslogSupervisor "wbinfomq" supervisor
    syslogInfo "starting rabbitmq client"
    startAMQP
    inbox <- selfMBox
    liftIO $! do
        installHandler sigTERM (Catch ( sendMBox inbox (toMessage WBSignalStop))) Nothing 
        installHandler sigHUP (Catch ( sendMBox inbox (toMessage WBSignalReload))) Nothing 
    procRunning
    

wbInfoShutdown:: HEPProc
wbInfoShutdown = do
    syslogInfo "stopping AMQP client"
    stopAMQP
    syslogInfo "stopping Syslog client"
    stopSyslog
    procFinished

wbInfoWorker:: HEPProc
wbInfoWorker = do
    msg <- receive
    case fromMessage msg of
        Nothing -> procRunning
        Just WBSignalStop -> procFinished


supervisor:: HEP HEPProcState
supervisor = do
    me <- self
    msg <- receive
    let handleChildLinkMessage:: Maybe LinkedMessage -> EitherT HEPProcState HEP HEPProcState
        handleChildLinkMessage Nothing = lift procRunning >>= right
        handleChildLinkMessage (Just (ProcessFinished pid)) = do
            liftIO $! putStrLn $! "I've been noticed about " ++ show pid
            subscribed <- lift getSubscribed
            case subscribed of
                [] -> lift procFinished >>= left
                _ -> lift procRunning >>= left
        
        handleServiceMessage:: Maybe SupervisorMessage -> EitherT HEPProcState HEP HEPProcState
        handleServiceMessage Nothing = lift procRunning >>= right
        handleServiceMessage (Just (ProcWorkerFailure cpid e state outbox)) = do
            liftIO $! putStrLn $! "supervisor worker: " ++ show e
            lift $! procContinue outbox
            lift procRunning >>= right
        handleServiceMessage (Just (ProcInitFailure cpid e _ outbox)) = do
            liftIO $! putStrLn $! "supervisor init: " ++ show e
            lift $! procContinue outbox
            lift procFinished >>= left
    mreq <- runEitherT $! do
        handleChildLinkMessage $! fromMessage msg
        handleServiceMessage $! fromMessage msg 
        
    case mreq of
        Left some -> return some
        Right some -> return some

data AMQPState = AMQPState
    { conn:: AMQP.Connection
    , chan:: AMQP.Channel
    }
    deriving Typeable
instance HEPLocalState AMQPState

data AMQPMessage = AMQPPing (MBox AMQPAnswer)
                 | AMQPStop
    deriving Typeable
instance HEP.Message AMQPMessage

data AMQPAnswer = AMQPAnswerOK

amqpProc = "AMQPMain"
amqpSupervisorProc = "AMQPSupervisor"

startAMQP:: HEP Pid
startAMQP = do
    spawn $! procRegister amqpProc $! 
        procWithSupervisor (procRegister amqpSupervisorProc $! proc amqpSupervisor) $! 
        procWithBracket  amqpInit amqpShutdown $! proc amqpWorker
    
    

amqpInit:: HEPProc
amqpInit = do
    _conn <- liftIO $! openConnection "127.0.0.1" "/" "guest" "guest"
    _chan <- liftIO $! openChannel _conn
    setLocalState $! Just $! AMQPState 
        { conn = _conn
        , chan = _chan
        }
    procRunning

amqpShutdown:: HEPProc
amqpShutdown = do
    s <- localState
    let shutdownAMQP:: AMQPState -> HEPProc
        shutdownAMQP state = do
            liftIO $! closeConnection (conn state)
            setLocalState (Nothing:: Maybe AMQPState)
            procFinished
    case s of
        Nothing -> procFinished
        Just state -> shutdownAMQP state



amqpSupervisor:: HEPProc
amqpSupervisor = do
    msg <- receive
    let handleChildLinkMessage:: Maybe LinkedMessage -> EitherT HEPProcState HEP HEPProcState
        handleChildLinkMessage Nothing = lift procRunning >>= right
        handleChildLinkMessage (Just (ProcessFinished pid)) = do
            lift $! syslogInfo $! "supervisor: spotted client exit " ++ show pid
            subscribed <- lift getSubscribed
            case subscribed of
                [] -> lift procFinished >>= left
                _ -> lift procRunning >>= left
        
        handleServiceMessage:: Maybe SupervisorMessage -> EitherT HEPProcState HEP HEPProcState
        handleServiceMessage Nothing = lift procRunning >>= right
        handleServiceMessage (Just (ProcWorkerFailure cpid e _ outbox)) = do
            lift $! syslogError $! "supervisor: worker " ++ show cpid ++ 
                " failed with: " ++ show e ++ ". It will be recovered"
            lift $! procContinue outbox
            lift procRunning >>= left
        handleServiceMessage (Just (ProcInitFailure cpid e _ outbox)) = do
            lift $! syslogError $! "supervisor: init of " ++ show cpid ++ 
                " failed with: " ++ show e ++ ". It will be restarted after 10 seconds"
            liftIO $! threadDelay 10000000
            lift $! procRestart outbox Nothing
            lift procRunning >>= left
    mreq <- runEitherT $! do
        handleChildLinkMessage $! fromMessage msg
        handleServiceMessage $! fromMessage msg 
        
    case mreq of
        Left some -> return some
        Right some -> return some
    

amqpWorker = procFinished


stopAMQP:: HEP ()
stopAMQP = do
    send (toPid amqpProc) $! AMQPStop
    
    
    
