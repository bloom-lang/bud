require "bud/errors"
if RUBY_VERSION <= "1.9"
  require 'faster_csv'
  $mod = FasterCSV
else
  require 'csv'
  $mod = CSV
end

# Metrics are reported in a nested hash representing a collection of relational tables.
# The metrics hash has the following form:
# - key of the metrics hash is the name of the metric (table), e.g. "tickstats", "collections", "rules", etc.
# - value of the metrics is itself a hash holding the rows of the table, keyed by key columns.
# - It has the following form:
#   - key is a hash of key attributes, with the following form:
#     - key is the name of an attribute
#     - value is the attribute's value
#   - value is a single dependent value, e.g. a statistic like a count

def report_metrics 
  metrics.each do |k,v|
    if v.first
      csvstr = $mod.generate(:force_quotes=>true) do |csv|
        csv << [k.to_s] + v.first[0].keys + [:val]
        v.each do |row|
          csv << [nil] + row[0].values + [row[1]]
        end
      end
      puts csvstr
    end
  end
end

def initialize_stats
  return {{:name=>:count} => 0, {:name=>:mean} => 0, {:name=>:Q} => 0, {:name=>:stddev} => 0}
end
  
# see http://en.wikipedia.org/wiki/Standard_deviation#Rapid_calculation_methods
def running_stats(stats, elapsed)
  raise Bud::Error, "running_stats called with negative elapsed time" if elapsed < 0
  stats[{:name=>:count}] += 1
  oldmean = stats[{:name=>:mean}]
  stats[{:name=>:mean}] = stats[{:name=>:mean}] + \
                         (elapsed - stats[{:name=>:mean}]) / stats[{:name=>:count}]
  stats[{:name=>:Q}] = stats[{:name=>:Q}] + \
                       (elapsed - oldmean) * (elapsed - stats[{:name=>:mean}])
  stats[{:name=>:stddev}] = Math.sqrt(stats[{:name=>:Q}]/stats[{:name=>:count}])
  stats
end
