require "google/cloud/spanner"
require "google/cloud/spanner/admin/database"
require "google/cloud/spanner/admin/instance"

ENV["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/bajajnehaa/Desktop/service-accounts/sturdy-pier-323908-d0ab05f8aa92.json"
ENV["GOOGLE_CLOUD_PROJECT"] = "sturdy-pier-323908"

ENV["GOOGLE_CLOUD_PROJECT"] = "helical-zone-771"
ENV["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/bajajnehaa/Downloads/helical-zone-771-881ec1eacead.json"

config = Google::Cloud::Spanner.configure do |conf|
  conf.timeout = 15
end

config.timeout #15

spanner = Google::Cloud::Spanner.new

instance = spanner.instances.first

######## Database admin test ###############

client = Google::Cloud::Spanner::Admin::Database.database_admin project_id: instance.project_id

client.configure.timeout #15


daconf = Google::Cloud::Spanner::Admin::Database.configure do |config|
  config.timeout = 25
end

client_new = Google::Cloud::Spanner::Admin::Database.database_admin project_id: instance.project_id

client_new.configure.timeout #25


######## Instance admin test ###############

ENV["SPANNER_CREDENTIALS"]="/Users/bajajnehaa/Downloads/sturdy-pier-323908-95ae0d0da383.json"

client = Google::Cloud::Spanner::Admin::Instance.instance_admin project_id: instance.project_id

client.configure.timeout

instance_conf = Google::Cloud::Spanner::Admin::Instance.configure do |config|
  config.timeout = 25
end

client_new = Google::Cloud::Spanner::Admin::Instance.instance_admin project_id: instance.project_id

client_new.configure.timeout