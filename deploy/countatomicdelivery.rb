require 'rubygems'
require 'bud'

# XXX: delivery modules are a massive hack.  we need to think about
# aspect-oriented programming here, or allow users to extend the definitions of
# existing table types
module CountAtomicDelivery
  include BudModule

  state {
    scratch :atomic_data_in, [:loc, :tuple]
    channel :atomic_data_chan, [:@loc, :tuple]
    table :atomic_data_recv, [:loc, :tuple]

    scratch :atomic_count, [:loc, :cnt]
    channel :atomic_count_chan,[:@loc, :cnt]
    table :atomic_count_recv, [] => [:cnt]

    scratch :atomic_recv_count, [:loc] => [:cnt]
    scratch :atomic_data_atomic, [:loc, :tuple]
    scratch :atomic_data_out, [:tuple]
  }

  declare
  def countatomicdelivery
    atomic_count <= atomic_data_in.group([:loc], count)

    atomic_data_chan <~ atomic_data_in
    atomic_count_chan <~ atomic_count

    atomic_count_recv <= atomic_count_chan.map {|c| [c.cnt]}
    atomic_data_recv <= atomic_data_chan

    atomic_recv_count <= atomic_data_recv.group([:loc], count)

#    atomic_data_atomic <= join([atomic_recv_count, atomic_count_recv,
#                                atomic_data_recv],
#                               [atomic_recv_count.cnt,
#                                atomic_count_recv.cnt]).map{ |rc, cr, d| d }

    atomic_data_atomic <= ((if atomic_recv_count.first and
                                atomic_count_recv.first and
                                atomic_recv_count.first.cnt ==
                                atomic_count_recv.first.cnt
                              atomic_data_recv
                            end) or [])

    # commented out due to bug #85
    # atomic_data_recv <- atomic_data_atomic
    # idempotence hack inserted due to bug #85
    atomic_data_out <= atomic_data_atomic.map {|a| [a.tuple] if idempotent [[:atomic_data_out, a]]}
  end
end
