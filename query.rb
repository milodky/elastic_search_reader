require_relative 'elastic_searcher'
require 'elastic_search_parser'
require 'active_support/all'
require 'yaml'
require 'awesome_print'

module Person
  CONFIGURATION = YAML.load_file('person.yml')
  def self.find(params)
    params     = params.dup.with_indifferent_access
    conditions = params.delete(:conditions)
    ret = ElasticSearchParser::QueryParser.new(conditions, CONFIGURATION)
    DataDataAccess::ElasticSearcher.search(*self.search_params(ret, params))
  end

  def self.search_params(query_parser, options)
    url   = query_parser.url
    index = query_parser.index
    body  = query_parser.body
    routing = query_parser.routing
    ret = [{:body => body}, options, {:url => url, :index => index, :routing => routing}]
    ret
  end


end