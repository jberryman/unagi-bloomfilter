module Main(main) where

import qualified Control.Concurrent.BloomFilter as Bloom

main = do
    b_5_20 <- Bloom.new (Bloom.SipKey 1 1) 5 20
    p <- Bloom.lookup b_5_20 (1::Int)
    print p
