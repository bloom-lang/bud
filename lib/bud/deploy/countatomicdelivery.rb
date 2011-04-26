require 'rubygems'
require 'bud'

# XXX: delivery modules are a massive hack.  we need to think about
# aspect-oriented programming here, or allow users to extend the definitions of
# existing table types
module CountAtomicDelivery # :nodoc: all
  state do
    scratch :atomic_data_in, [:loc, :tuple]
    channel :atomic_data_chan, [:@loc, :tuple]
    table :atomic_data_recv, [:loc, :tuple]

    channel :atomic_count_chan,[:@loc, :cnt]
    table :atomic_count_recv, [] => [:cnt]

    scratch :atomic_recv_count, [:loc] => [:cnt]
    scratch :atomic_data, [:loc, :tuple]
    scratch :atomic_data_out, [:tuple]
  end

  bloom :countatomicdelivery do
    temp :atomic_count <= atomic_data_in.group([:loc], count)

    atomic_data_chan <~ atomic_data_in
    atomic_count_chan <~ atomic_count

    atomic_count_recv <= atomic_count_chan {|c| [c.cnt]}
    atomic_data_recv <= atomic_data_chan

    atomic_recv_count <= atomic_data_recv.group([:loc], count)

    atomic_data <= (atomic_recv_count * atomic_count_recv * atomic_data_recv).combos(atomic_recv_count.cnt => atomic_count_recv.cnt) {|rc, cr, d| d}

    atomic_data_recv <- atomic_data

    atomic_data_out <= atomic_data {|a| [a.tuple]}
  end
end
