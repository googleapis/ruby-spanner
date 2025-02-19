# -*- encoding: utf-8 -*-
require File.expand_path("../lib/google/cloud/spanner/version", __FILE__)

Gem::Specification.new do |gem|
  gem.name          = "google-cloud-spanner"
  gem.version       = Google::Cloud::Spanner::VERSION

  gem.authors       = ["Mike Moore", "Chris Smith"]
  gem.email         = ["mike@blowmage.com", "quartzmo@gmail.com"]
  gem.description   = "google-cloud-spanner is the official library for Google Cloud Spanner API."
  gem.summary       = "API Client library for Google Cloud Spanner API"
  gem.homepage      = "https://github.com/googleapis/ruby-spanner/blob/main/google-cloud-spanner"
  gem.license       = "Apache-2.0"

  gem.files         = `git ls-files -- lib/*`.split("\n") +
                      ["OVERVIEW.md", "AUTHENTICATION.md", "LOGGING.md", "CONTRIBUTING.md", "TROUBLESHOOTING.md", "CHANGELOG.md", "CODE_OF_CONDUCT.md", "LICENSE", ".yardopts"]
  gem.require_paths = ["lib"]

  gem.required_ruby_version = ">= 3.0"

  gem.add_dependency "bigdecimal", "~> 3.0"
  gem.add_dependency "concurrent-ruby", "~> 1.0"
  gem.add_dependency "google-cloud-core", "~> 1.7"
  gem.add_dependency "google-cloud-spanner-admin-database-v1", "~> 1.6"
  gem.add_dependency "google-cloud-spanner-admin-instance-v1", "~> 2.0"
  gem.add_dependency "google-cloud-spanner-v1", "~> 1.6"
end
