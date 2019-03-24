class HomepageController < ApplicationController
  def index
    @country_stats = CountryStat.order('visit_count desc')
    @map = {}
    @country_stats.each do |country_data|
      @map[country_data.country_code] = country_data.visit_count
    end
  end
end
