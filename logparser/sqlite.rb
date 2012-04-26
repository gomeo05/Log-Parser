# Copyright (c) 2012 Xero Ltd
# 
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
# 
# The above copyright notice and this permission notice shall
# be included in all copies or substantial portions of the
# Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

require 'logparser/sink'
require 'sqlite3'
require 'pp'

module LogParser

  class SQLite < Sink
    attr_reader :table, :id

    def initialize opts
      opts = { :table => 'logs', :master => nil, :db => ':memory:' }.merge opts
      super()
      if opts[:master].nil?
        @db = SQLite3::Database.new(opts[:db])
        init_functions
      else
        @db = opts[:master].db
      end
      @columns = [ 'id' ]
      @columndefs = { 'id' => 'id' }
      @id = 0
      @table = opts[:table]

      @transforms = {
        :sc_status => :integer,
        :vip => :integer,
        :time_taken => :integer
      }
      execute "CREATE TABLE #{table} ( id )"
      @last_result = nil
      @insert = nil
    end

    def close
      @last_result.close unless @last_result.nil?
      begin
        db.close
      rescue Object => e
        log :error, "can't close database #{e}, dropping table instead"
        execute "DROP TABLE #{table}"
        @db = nil
      end
      super
    end

    def db
      @db
    end

    def fields_filter fields
      if fields.nil?
        @insert = nil
        return
      end
      fields = fields.map { |x| x.gsub(/[-()]/,'_') }
      new_table = @id == 0
      columns = ['id']
      fields.each do |field|
        unless @columns.include? field
          column_def = "#{field} DEFAULT NULL"
          if new_table
            columns.push column_def
          else
            execute "ALTER TABLE #{table} ADD COLUMN #{column_def}"
          end
          @columndefs[field] = column_def
          @columns.push field
        end
      end
      if new_table
        execute "DROP TABLE #{table}"
        coldefs = @columns.map { |x| @columndefs[x] }
        execute "CREATE TABLE #{table} (#{coldefs.join ','})"
      end
      qm = fields.map do |field|
        transform = @transforms[field.to_sym]
        if transform.nil?
          "?"
        else
          "CAST (? AS #{transform})"
        end
      end
      insert_string = "INSERT INTO #{table} (id, #{fields.join ','}) VALUES (?, #{qm.join ','})"
      @last_result.close unless @last_result.nil?
      @last_result = nil
      @insert = @db.prepare insert_string
      if @write_size != fields.size
        @write_size = fields.size
        write_string = "def write data\n@insert.bind_param 1,@id\n"
        @write_size.times do |index|
          write_string += "@insert.bind_param #{index+2}, data[#{index}]\n"
        end
        write_string += "@last_result = @insert.execute\n@insert.reset!\n@id = @id.succ\ntrue\nend\n"
        eval write_string
      end
      true
    end

    def write data
      throw "should never be reached"
      @insert.bind_param 1, @id
      data.size.times do |index|
        @insert.bind_param(2+index, data[index])
      end
      @last_result = @insert.execute #@id, *data
      @insert.reset!
      @id = @id.succ
      true
    end

    private

    def execute *args
      #puts args
      @db.execute *args
    end

    def percentile_object percentile
      aggregator = Array.new
      def aggregator.percentile= x
        @percentile = x / 100.0
      end
      def aggregator.step value
        self.push value
      end
      def aggregator.finalize
        if empty?
          nil
        else
          result = sort[(size - 1) * @percentile + 0.5]
          clear
          result
        end
      end
      aggregator.percentile= percentile
      aggregator
    end

    def init_percentile_function
      @gc_hates_us ||= []
      [25,50,75,95,99].each do |percentile|
        aggregator = percentile_object(percentile)
        @db.define_aggregator "percentile#{percentile}", aggregator
        @gc_hates_us.push aggregator
      end
    end


    def init_functions
      init_percentile_function
      init_function "ext" do |fn|
        if fn =~ /\.([a-z0-9]+)$/i
          $1.downcase
        else
          ''
        end
      end
      init_function "uri2graphite" do |fn|
        fn.gsub /[^a-zA-Z0-9,\-]/, '_'
      end
      init_function "subnet" do |x|
        x.sub /\.[0-9]+$/, ''
      end
    end

    def init_function name, &block
      @gc_hates_us ||= []
      @gc_hates_us.push block
      @gc_hates_us.push name
      @db.define_function name, &block
    end
  end

end