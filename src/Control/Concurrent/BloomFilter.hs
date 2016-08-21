module Control.Concurrent.BloomFilter (
{- | A thread-safe mutable bloom filter. Some additional functionality for
   advanced users is exposed in "Control.Concurrent.BloomFilter.Internal".
 -}
      BloomFilter()
    , BloomFilterException(..)
    -- * Creation
    , new
    , SipKey(..)
    -- * Operations
    , insert
    , lookup
    -- ** Copying and Combining
    , unionInto
    , intersectionInto
    , clone
    -- * Serialization
    , serialize
    , deserialize
    -- * Utilities
    , fpr

  ) where

import Control.Concurrent.BloomFilter.Internal
import Prelude hiding (lookup)
