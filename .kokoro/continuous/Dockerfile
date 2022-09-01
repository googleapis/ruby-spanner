FROM ruby:3.1.1 AS build-env

ARG RAILS_ROOT=/app
ARG BUILD_PACKAGES="git"
ARG DEV_PACKAGES="postgresql-client"
ARG RUBY_PACKAGES="tzdata"

ENV BUNDLE_APP_CONFIG="$RAILS_ROOT/.bundle"

WORKDIR $RAILS_ROOT
# install packages
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y $BUILD_PACKAGES $DEV_PACKAGES $RUBY_PACKAGES
COPY Gemfile* ./

# install rubygem
COPY Gemfile Gemfile.lock $RAILS_ROOT/

RUN gem install bundler \
    && bundle config set --local path 'vendor/bundle'

RUN bundle config --global frozen 1 \
    && bundle install -j4 --retry 3 \
    # Remove unneeded files (cached *.gem, *.o, *.c)
    && rm -rf vendor/bundle/ruby/3.1.0/cache/*.gem \
    && find vendor/bundle/ruby/3.1.0/gems/ -name "*.c" -delete \
    && find vendor/bundle/ruby/3.1.0/gems/ -name "*.o" -delete

COPY . .

# Remove folders not needed in resulting image
RUN rm -rf tmp/cache app/assets vendor/assets spec




############### Build step done ###############
FROM ruby:3.1.1

ARG RAILS_ROOT=/app
ARG PACKAGES="tzdata postgresql-client bash git imagemagick"

ENV BUNDLE_APP_CONFIG="$RAILS_ROOT/.bundle"

WORKDIR $RAILS_ROOT

# install packages
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y $PACKAGESS

COPY --from=build-env $RAILS_ROOT $RAILS_ROOT

# Add a script to be executed every time the container starts.
COPY web_entrypoint.sh /usr/bin/
RUN chmod +x /usr/bin/web_entrypoint.sh
ENTRYPOINT ["web_entrypoint.sh"]

CMD ["bundle", "exec", "rails", "server", "-b", "0.0.0.0", "-p", "3000"]