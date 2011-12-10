class Bud::NonceGenerator
  def initialize(prefix)
    @prefix = prefix
    @cnt = 0
    @memory = {}
  end

  def reset
    @memory.clear
  end

  def generate(v)
    @memory[v] ||= make_nonce
    @memory[v]
  end

  private
  def make_nonce
    rv = Bud::SafeNonce.new(@prefix, @cnt)
    @cnt += 1
    rv
  end
end

class Bud::SafeNonce
  def initialize(prefix, cnt)
    @prefix = prefix
    @cnt = cnt
  end

  def inspect
    "<#{@prefix},#{@cnt}>"
  end
end
