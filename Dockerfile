FROM node:10.15.3-alpine
ENV NODE_ENV production
RUN mkdir -p /home/creator_detail
WORKDIR /home/creator_detail
ADD . /home/creator_detail
RUN yarn
EXPOSE 4000
CMD NODE_ENV=production yarn start