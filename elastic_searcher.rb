require 'elasticsearch-api'
require 'faraday'
module DataDataAccess
  DEFAULT_SCROLL_PARAM     = '5m'
  DEFAULT_BODY             = {:query => {:match_all => {}}}
  PRIMARY_SEARCH_PARAMS    = [:index, :type, :routing]
  ADDITIONAL_SEARCH_PARAMS = [:order, :fields, :search_type, :preference, :sort]
  DEBUG = false
  class ElasticSearchInternalError < StandardError
    attr_reader :es_result
    def initialize(error_message, es_result)
      @es_result = es_result
      super(error_message)
    end
  end

  module Utility
    def self.exponential_sleep(current_retry)
      puts "Current retry: #{current_retry}" if DataDataAccess::DEBUG
      sleep((2 ** current_retry ).to_f / 1.5)
      current_retry + 1
    end
  end

  class ElasticSearcher

    include Elasticsearch::API
    DEFAULT_FROM    = 0
    DEFAULT_SIZE    = 10
    REGEX_QUERY     = []
    MAX_RETRY_LIMIT = 4
    DEFAULT_SCAN_SIZE = 1000

    #################################################
    # this method will overwrite the original one
    # defined in Elasticsearch::API and perform the
    # request
    #
    def perform_request(action, path, params, body)
      raise ArgumentError.new('Endpoint not set!') if es_url.nil?
      {:a => :b}.to_json
      body = MultiJson.dump(body) if body && !body.is_a?(String)

      if DataDataAccess::DEBUG
        puts action.inspect
        puts path.inspect
        puts params.inspect
        puts body.inspect
      end
      conditions          = {url: es_url}
      conditions[:params] = params unless params.blank?
      connection          = ::Faraday::Connection.new(conditions)

      connection.run_request(action.downcase.to_sym, path, body, {'Content-Type' => 'application/json'})
    end

    # get the url
    def es_url; @es_url; end
    # set the url
    def es_url=(_url); @es_url = _url; end

    ################################################
    # class methods
    #
    def self.create_index(params, config)
      puts request(:action => :create_index, :params => params, :config => config)
    end


    def self.put_mapping(params, config)
      request(:action => :put_mapping, :params => params, :config => config)
    end


    def self.delete_index(params, config)
      puts request(:action => :delete_index, :params => params, :config => config)
    end
    class << self
      public

      def get_mapping(config = {}); request(:action => :get_mapping, :config => config); end

      ##############################################
      # query the elastic search server
      #
      # Parameter:
      # - entry_hash: a hash containing the query conditions. E.X. => {first_name: 'abc', last_name: 'def'}
      # - page:       the page to search
      # - size:       the return size
      #
      # Return:
      # the total count of matched items and an array
      # items with specified size
      #
      def search(es_params, options = {}, config = {})
        params = HashWithIndifferentAccess.new

        # primary search params: index,type and routing etc
        PRIMARY_SEARCH_PARAMS.each    {|key| params[key] = config[key]  if config[key].present?}
        ADDITIONAL_SEARCH_PARAMS.each {|key| params[key] = options[key] if options[key].present?}
        params[:body] = es_params.fetch(:body, DEFAULT_BODY)
        params[:from] = options.fetch(:from, DEFAULT_FROM)
        params[:size] = options.fetch(:size, DEFAULT_SIZE)
        ap "params for es search: #{params.inspect}" if DataDataAccess::DEBUG
        result        = request(:action => :search, :params => params, :config => config)
        puts "result from es search: #{result.inspect}" if DataDataAccess::DEBUG
        {:hits => result['hits']['hits'], :total => result['hits']['total'],
         :aggregation => result['aggregations'], :took => result['took']}
      end

      ###########################################################
      #
      def multi_search(body, config = {})
        responses = request(:action => :msearch, :params => {:body => body}.deep_symbolize_keys, :config => config)[:responses]
        responses.map {|response| response['hits']['hits']}
      end

      ###########################################################
      # save the entries into ES
      #
      # Parameter:
      # - etnries: an array containing data to be saved to elastic search
      # Input:
      # [{:data => {}, :index => 'abc', :type => 'pa', :id => '12345'}...{}], (In each hash, data field is a must)
      # Example:
      # [{:data=>{:first_name=>"x", :last_name=>"abcdefg", :_id=>"12349191227273421", :shard_key=>"ab"}, :id=>"12349191227273421"}]
      # Return:
      # No return value.
      #
      def save(entries, config)
        bulk(entries, :index, config)
      end
      ###########################################################
      # save the entries into ES
      #
      # Parameter:
      # - entries: an array containing data to be updated to elastic search
      # Input:
      # [{:data => {}, :index => 'abc', :type => 'pa', :id => '12345'}...{}], (In each hash, data and id are must)
      # Example:
      # [{:data=>{:first_name=>"x", :last_name=>"abcdefg", :_id=>"12349191227273421", :shard_key=>"ab"}, :id=>"12349191227273421"}]
      # Return:
      # No return value.
      #
      # Seldom used
      #
      def update(entries)
        data_in_hash = {}
        entries.each do |entry|
          entry = entry.deep_symbolize_keys
          _index = entry[:_index]
          _type  = entry[:_type]
          _id    = entry[:_id]
          data  = entry[:data]

          data_in_hash[index]       ||= {}
          data_in_hash[index][type] ||= []
          data_in_hash[index][type] << {:update => {_index: _index, _type: _type, _id: _id, data: {doc: data}}}
        end

        data_in_hash.each do |index, items|
          items.each do |type, body|
            params = {body: body, index: index, type: type}
            request(:action => :bulk, :params => params)
          end
        end
        nil
      end

      ###########################################################
      # delete one entry from ES
      #
      # Parameter:
      # - params: a hash that contains:
      #           - id: the id of the entry in elastic search
      #
      # Return:
      # No return value.
      #
      def delete(entries, config = {})
        bulk(entries, :delete, config)
      end

      def bulk(entries, method, config = {})
        data_in_hash   = {}
        failed_entries = []
        # put the data into specific categories
        entries.each do |entry|
          entry  = entry.deep_symbolize_keys
          _index = entry[:_index]
          _type  = entry[:_type]
          data_in_hash[_index]        ||= {}
          data_in_hash[_index][_type] ||= []
          data_in_hash[_index][_type] << {method => entry}
        end

        data_in_hash.each do |index, items|
          items.each do |type, body|
            params        = {body: body, index: index, type: type}
            current_retry = 1
            begin
              result = request(:action => :bulk, :params => params, :config => config)
            rescue ElasticSearchInternalError => err
              errors = err.es_result
              if errors['errors'] && current_retry <= MAX_RETRY_LIMIT
                current_retry = Utility.exponential_sleep(current_retry)
                # take those error cases out of the errors
                failed_ids  = Set.new(errors['items'].map{|item| item[method.to_s]['_id'] if item[method.to_s]['error']}.compact)
                failed_body = params[:body].select{|item| failed_ids.include?(item[method][:_id])}
                params      = {:body => failed_body, :index => index, :type => type}
                retry
              end

              config[:logger].fatal("Fatal #{method} Error: #{errors}") if config[:logger] rescue nil
              failed_entries += errors['items'].select{|item| item[method.to_s]['error']}
            end
          end
        end
        failed_entries
      end

      ###########################################################
      # scan
      #
      def scan(params)
        index     = params[:index]
        scroll    = params[:scroll] || DEFAULT_SCROLL_PARAM
        body      = params[:body] || { query: { match_all: {} }}
        scroll_id = params[:scroll_id]
        size      = params[:size] || DEFAULT_SCAN_SIZE

        client = self.new
        client.es_url = params[:url]

        result = scroll_id ? client.scroll(scroll_id: scroll_id, scroll: scroll) :
            client.search(:index => index, :scroll => scroll, body: body, size: size)

        scroll_hash = JSON.parse(result)
        scroll_id = scroll_hash['_scroll_id']
        ret = scroll_hash['hits']['hits']

        ret.each do |data|
          data.delete('_score')
          data[:data] = data.delete('_source')
        end

        {:items => ret, :scroll_id => scroll_id}
      end

      protected

      ###########################################################
      # send the request to elastic search server
      # DO NOT Touch it
      #
      def request(conditions)
        raise ArgumentError.new('Input should be a hash') unless conditions.is_a?(Hash)
        conditions = conditions.deep_symbolize_keys
        action     = conditions[:action]
        params     = conditions[:params]
        config     = conditions[:config]
        client     = self.new
        client.es_url = config[:url]
        puts "endpoint: #{config[:url].inspect}" if DataDataAccess::DEBUG
        result =
            case action
              when :search         then client.search(params)
              when :bulk           then client.bulk(params)
              when :delete         then client.delete(params)
              when :msearch        then client.msearch(params)
              when :get_mapping    then client.indices.get_mapping
              when :create_index   then client.indices.create(params)
              when :put_mapping    then client.indices.put_mapping(params)
              when :delete_index   then client.indices.delete(params)
              else
                if action.respond_to?(:to_sym) && client.respond_to?(action.to_sym)
                  client.send(action.to_sym, *params)
                else
                  raise ArgumentError.new("Undefined action: #{action}")
                end
            end
        parsed_result = JSON.parse(result).with_indifferent_access
        puts "elasticsearch return for #{action}: #{parsed_result.inspect}" if DataDataAccess::DEBUG
        # raise an exception if there is any error
        error = parsed_result['error'] || parsed_result['errors']
        raise ElasticSearchInternalError.new(error, parsed_result)  if error
        parsed_result
      end
    end
  end
end
