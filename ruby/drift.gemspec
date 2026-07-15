Gem::Specification.new do |spec|
  spec.name          = "drift-sdk"
  spec.version       = "3.0.0"
  spec.authors       = ["Drift"]

  spec.summary       = "Drift SDK for Ruby Atomic functions"
  spec.description   = "Single-file, zero-dependency Ruby SDK for writing Drift Atomic functions."
  spec.homepage      = "https://ondrift.eu"
  spec.license       = "MIT"
  spec.metadata = {
    "source_code_uri" => "https://github.com/ondrift/sdk",
    "documentation_uri" => "https://ondrift.eu/docs",
    "bug_tracker_uri" => "https://github.com/ondrift/sdk/issues",
  }

  spec.required_ruby_version = ">= 3.0"

  spec.files = [
    "drift.rb",
  ]
  spec.require_paths = ["."]
end
