# TODO:
#  * use binary search to locate target bucket efficiently
#  * use a linked list to avoid array copies on insertion
#    (seems in conflict w/ binary search; hybrid solutions possible)
class Bud::MultiRange
  include Enumerable

  Bucket = Struct.new(:lo, :hi)

  def initialize(v)
    @buckets = [Bucket.new(v, v)]
  end

  def each
    @buckets.each do |b|
      (b.lo..b.hi).each do |v|
        yield v
      end
    end
  end

  def inspect
    @buckets.inspect
  end

  def nbuckets
    @buckets.size
  end

  def nvalues
    @buckets.map{|b| 1 + b.hi - b.lo}.reduce(:+)
  end

  def <<(v)
    @buckets.each_with_index do |b,i|
      if v >= b.lo
        if v <= b.hi
          return self   # In between lo and hi (inclusive)
        else
          next          # Greater than both lo and hi
        end
      else
        # Smaller than lo; either decrease lo (if possible) or create a new
        # bucket at offset i-1.
        if v + 1 == b.lo
          # Check whether we need to merge adjacent buckets
          @buckets[i].lo = v

          if i > 0 and @buckets[i].lo == (@buckets[i-1].hi + 1)
            merge_bucket(i)
          end
        else
          if i > 0 and @buckets[i-1].hi + 1 == v
            @buckets[i-1].hi = v
          else
            # Create a new bucket at offset i-1
            @buckets.insert(i, Bucket.new(v, v))
          end
        end

        return self
      end
    end

    # We didn't find a match, so extend the last bucket or add a new one
    last = @buckets.last
    if last.hi + 1 == v
      last.hi = v
    else
      @buckets << Bucket.new(v, v)
    end
    self
  end

  def merge_bucket(i)
    @buckets[i-1].hi = @buckets[i].hi
    @buckets.delete_at(i)
  end
end
