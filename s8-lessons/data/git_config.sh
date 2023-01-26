echo "Git configuration handler"
echo "Enter user name:"
read name
echo "Enter email:"
read email

git config --global user.name $name
git config --global user.email $email