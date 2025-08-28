# Gebruik een lichte Node.js image
FROM node:18-alpine

# Maak app directory
WORKDIR /usr/src/app

# Kopieer package.json en installeer dependencies
COPY package.json ./
RUN npm install --production

# Kopieer de rest
COPY . .

# Expose port
EXPOSE 3000

# Start command
CMD ["npm", "start"]
