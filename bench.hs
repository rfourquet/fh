{-# LANGUAGE OverloadedStrings #-}

import Criterion.Main  (bench, defaultMain, nf, whnf)
import Data.ByteString (ByteString)

import Hashing


emptySha1Key :: ByteString
emptySha1Key = ".git/annex/objects/Jx/11/SHA1-s0--da39a3ee5e6b4b0d3255bfef95601890afd80709/SHA1-s0--da39a3ee5e6b4b0d3255bfef95601890afd80709"

getHash :: ByteString -> Maybe ByteString
getHash  = fmap snd . getAnnexSizeAndHash

main :: IO ()
main = defaultMain
       [ bench "getAnnexSizeAndHash whnf"  $ whnf getAnnexSizeAndHash emptySha1Key
       , bench "getAnnexSizeAndHash nf"    $ nf   getAnnexSizeAndHash emptySha1Key
       , bench "getHash whnf"              $ whnf getHash             emptySha1Key
       , bench "getHash nf"                $ nf   getHash             emptySha1Key
       ]
