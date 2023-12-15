# Sử dụng hình ảnh chính thức Node.js
FROM node:20.10.0

# Tạo thư mục ứng dụng
WORKDIR /usr/src/app

# Sao chép package.json và package-lock.json để tận dụng Docker cache
COPY package*.json ./

# Cài đặt dependencies
RUN npm install

# Sao chép mã nguồn ứng dụng vào hình ảnh Docker
COPY . .

# Mở cổng 80
EXPOSE 80

# Lệnh chạy ứng dụng khi container khởi chạy
CMD ["npm", "start"]