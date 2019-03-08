{-# LANGUAGE OverloadedStrings #-}

module RawPath (combine, combine', getSymbolicLinkStatus, listDirectory, pathSeparator, readFile,
               readFileLazy, readSymbolicLink, takeDirectory, takeFileName, (</>)) where

import qualified Data.ByteString               as B
import qualified Data.ByteString.Lazy          as L
import           Data.ByteString.RawFilePath   (readFile)
import           Data.Word                     (Word8)
import           Prelude                       hiding (readFile)
import           RawFilePath.Directory         (listDirectory)
import           System.Posix.ByteString       (RawFilePath)
import           System.Posix.Files.ByteString (getSymbolicLinkStatus, readSymbolicLink)
import           System.Posix.IO.ByteString    (OpenMode (ReadOnly), defaultFileFlags, fdToHandle,
                                                openFd)

pathSeparator :: Word8
pathSeparator = 0x2f -- '\n'

isPathSeparator :: Word8 -> Bool
isPathSeparator = (== pathSeparator)

takeFileName :: RawFilePath -> RawFilePath
takeFileName = snd . B.breakEnd isPathSeparator

takeDirectory :: RawFilePath -> RawFilePath
takeDirectory p = let dir = fst . B.breakEnd isPathSeparator $ p
                  in if B.null dir
                       then "."
                       else B.init dir

-- | Same as combine (</>) but doesn't check whether the second path starts with a path separator
-- | (i.e. the two paths are always concatenated).
combine' :: RawFilePath -> RawFilePath -> RawFilePath
combine' p q | B.null p                  = q
             | B.last p == pathSeparator = B.append p q
             | otherwise                 = B.intercalate (B.singleton pathSeparator) [p, q]

(</>), combine :: RawFilePath -> RawFilePath -> RawFilePath
combine p q | B.null q                  = p
            | B.head q == pathSeparator = q
            | otherwise                 = combine' p q

(</>) = combine


-- using Data.ByteString.RawFilePath.withFile doesn't work, as the handle can be closed prematurely
readFileLazy :: RawFilePath -> IO L.ByteString
readFileLazy path = openFd path ReadOnly Nothing defaultFileFlags >>= fdToHandle >>= L.hGetContents
