{-# LANGUAGE CPP , OverloadedStrings #-}
module Main where
#  ifdef ASSERTIONS_ON
#    error "Sorry, please reconfigure without -finstrumented so that we turn off assertions in library code."
#  endif

import Criterion.Main
import Control.DeepSeq(deepseq)
import Control.Monad
import qualified Data.Text as T

import Control.Concurrent.BloomFilter.Internal
import qualified Control.Concurrent.BloomFilter as Bloom
import Data.Hashabler

import qualified Data.Set as Set
import qualified Data.HashSet as HashSet

-- import System.IO.Unsafe(unsafePerformIO)
import System.Random

-- TODO comparisons with:
--   - pure Set
--   - best in class Int (or other specialized) hash map or trie
--   - general hashmap (of Hashable things)
--   - the above, wrapped in an IORef or MVar

main :: IO ()
main = do
    assertionsOn <- assertionCanary
    when assertionsOn $
      putStrLn  $ "!!! WARNING !!! assertions are enabled in library code and may result in "
                ++"slower than realistic benchmarks. Try configuring without -finstrumented"
    b_5_20 <- Bloom.new (SipKey 1 1) 5 20
    b_13_20 <- Bloom.new (SipKey 1 1) 13 20 -- needs 128

    -- This has 0.3% fpr for 10000 elements, so I think can be fairly compared
    b_text <- Bloom.new (SipKey 11 22) 3 12
    let txt = "orange" :: T.Text

    let g = mkStdGen 8973459
        chars = randoms g :: [Char]
        fakeWords = go chars
        go :: [Char] -> [String]
        go [] = error "noninfinite list"
        go (s:ss) = let (a,as) = splitAt 3 ss
                        (b,bs) = splitAt 5 as
                        (c,cs) = splitAt 5 bs
                        (d,ds) = splitAt 6 cs
                        (e,es) = splitAt 8 ds
                     in [s]:a:b:c:d:e:(go es)

    let textWords10k = map T.pack $ take 10000 fakeWords
    deepseq textWords10k $ return ()

    let hashset10 = HashSet.fromList $ take 10 textWords10k
    let hashset100 = HashSet.fromList $ take 100 textWords10k
    let hashset10000 = HashSet.fromList $ take 10000 textWords10k
    let set10 = Set.fromList $ take 10 textWords10k
    let set100 = Set.fromList $ take 100 textWords10k
    let set10000 = Set.fromList $ take 10000 textWords10k
    


    defaultMain [
      bgroup "internals" [
          bench "membershipWordAndBits64" $ nf (membershipWordAndBits64 (Hash64 1)) b_5_20
        , bench "membershipWordAndBits128" $ nf (membershipWordAndBits128 (Hash128 1 1)) b_13_20
        ],
      bgroup "lookup insert" [
          bench "siphash64 for comparison" $ whnf (siphash64 (SipKey 1 1)) (1::Int)

          -- best case, with no cache effects (I think):
        , bench "lookup (64)" $ whnfIO (Bloom.lookup b_5_20 (1::Int))
        , bench "lookup x10 (64)" $ nfIO (mapM_ (Bloom.lookup b_5_20) [1..10])
        , bench "lookup x100 (64)" $ nfIO (mapM_ (Bloom.lookup b_5_20) [1..100])
        , bench "lookup (128)" $ whnfIO (Bloom.lookup b_13_20 (1::Int))
        , bench "lookup x10 (128)" $ nfIO (mapM_ (Bloom.lookup b_13_20) [1..10])
        , bench "lookup x100 (128)" $ nfIO (mapM_ (Bloom.lookup b_13_20) [1..100])

        , bench "insert (64)" $ whnfIO (Bloom.insert b_5_20 (1::Int))
        , bench "insert x10 (64)" $ nfIO (mapM_ (Bloom.insert b_5_20) [1..10])
        , bench "insert x100 (64)" $ nfIO (mapM_ (Bloom.insert b_5_20) [1..100])
        , bench "insert (128)" $ whnfIO (Bloom.insert b_13_20 (1::Int))
        , bench "insert x10 (128)" $ nfIO (mapM_ (Bloom.insert b_13_20) [1..10])
        , bench "insert x100 (128)" $ nfIO (mapM_ (Bloom.insert b_13_20) [1..100])

        ],
      bgroup "comparisons micro" [
          bench "(just siphash64 on txt for below)" $ whnf (siphash64 (SipKey 1 1)) ("orange"::T.Text)
        , bench "Bloom.insert (64)" $ whnfIO (Bloom.insert b_text txt)
        {- I was concerned that the above might not be valid (perhaps the
         - hashing of the Text value was getting reused?), but the following
         - convinced me it's all right; we can see differences in size of input
         - string reflected in all these benchmarks. I believe bloomInsertPure1
         - reflects the inability to inline Hashable instance machinery (since
         - it must remain polymorphic.
        , bench "Bloom.insert (64)(validation1)" $ whnf (bloomInsertPure1 b_text) txt
        , bench "Bloom.insert (64)(validation2)" $ whnf (bloomInsertPure2 b_text) txt
        , bench "Bloom.insert (64)(validation3)" $ whnfIO (Bloom.insert b_text "ora")
        , bench "Bloom.insert (64)(validation4)" $ whnf (bloomInsertPure1 b_text) "ora"
        , bench "Bloom.insert (64)(validation5)" $ whnf (bloomInsertPure2 b_text) "ora"
        , bench "(validation orange)" $ whnf (siphash64 (SipKey 1 1)) ("orange"::T.Text)
        , bench "(validation ora)" $ whnf (siphash64 (SipKey 1 1)) ("ora"::T.Text)
        -}

        , bench "Set.insert into 10" $ whnf (\t-> Set.insert t set10) txt
        , bench "Set.insert into 100" $ whnf (\t-> Set.insert t set100) txt
        , bench "Set.insert into 10000" $ whnf (\t-> Set.insert t set10000) txt

        , bench "HashSet.insert into 10" $ whnf (\t-> HashSet.insert t hashset10) txt
        , bench "HashSet.insert into 100" $ whnf (\t-> HashSet.insert t hashset100) txt
        , bench "HashSet.insert into 10000" $ whnf (\t-> HashSet.insert t hashset10000) txt

        , bench "Bloom.lookup (64)" $ whnfIO (Bloom.lookup b_text txt)

        , bench "Set.member of 10" $ whnf (\t-> Set.member t set10) txt
        , bench "Set.member of 100" $ whnf (\t-> Set.member t set100) txt
        , bench "Set.member of 10000" $ whnf (\t-> Set.member t set10000) txt

        , bench "HashSet.member of 10" $ whnf (\t-> HashSet.member t hashset10) txt
        , bench "HashSet.member of 100" $ whnf (\t-> HashSet.member t hashset100) txt
        , bench "HashSet.member of 10000" $ whnf (\t-> HashSet.member t hashset10000) txt
      ],
      bgroup "comparisons big" [
      ],
      -- TODO large random lookup and insert benchmark, comparing with single-thread and then with work split.
      --      make this how we compare as well?
      --      Do this for various types of elements

      bgroup "combining and creation" [
        -- These timings can be subtracted from union timings:
          bench "new 14" $ whnfIO $ Bloom.new (SipKey 1 1) 3 14
        , bench "new 20" $ whnfIO $ Bloom.new (SipKey 1 1) 3 20

        , bench "unionInto (14 -> 14)" $ whnfIO $ unionBench 14 14
        , bench "unionInto (20 -> 14)" $ whnfIO $ unionBench 20 14 -- 20 is 6x
        , bench "unionInto (20 -> 20)" $ whnfIO $ unionBench 20 20
        ]
      ]

unionBench :: Int -> Int -> IO ()
unionBench bigl littlel = do
    b1 <- Bloom.new (SipKey 1 1) 3 bigl
    b2 <- Bloom.new (SipKey 1 1) 3 littlel
    b1 `Bloom.unionInto` b2


{-
-- So we can use whnf, and make sure hashes aren't being cached
{-# NOINLINE bloomInsertPure1 #-}
bloomInsertPure1 :: Hashable a => BloomFilter a -> a -> Bool
bloomInsertPure1 b = unsafePerformIO . Bloom.insert b

bloomInsertPure2 :: Hashable a => BloomFilter a -> a -> Bool
bloomInsertPure2 b = unsafePerformIO . Bloom.insert b
-}
