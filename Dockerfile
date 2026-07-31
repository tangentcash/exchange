FROM node:22-alpine AS build
WORKDIR /home/make
RUN apk add git
COPY ./ /home/make
RUN yarn && yarn make

FROM node:22-alpine AS deploy
WORKDIR /home/make
COPY --from=build /home/make /home/make
EXPOSE 19420
ENTRYPOINT ["yarn", "indexer", "/etc/tangentexchange.json"]