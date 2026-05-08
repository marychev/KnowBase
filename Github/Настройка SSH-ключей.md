# github.md



Настройка SSH-ключей позволит вам пушить код в GitHub без постоянного ввода логина и токена.
## 1. Генерация нового SSH-ключа
В терминале WSL выполните команду (замените email на свой):

ssh-keygen -t ed25519 -C "your_email@example.com"


* На запрос «Enter file in which to save the key» просто нажмите Enter (сохранит в стандартную папку ~/.ssh/).
* На запрос «Enter passphrase» можно нажать Enter (без пароля) или ввести пароль для дополнительной защиты самого файла ключа. [1, 2, 3, 4] 
* 

## 2. Добавление ключа в GitHub
Теперь нужно скопировать содержимое созданного публичного ключа:

   1. Выведите текст ключа на экран:
   
   cat ~/.ssh/id_ed25519.pub
   
   2. Скопируйте весь вывод (начинается с ssh-ed25519 и заканчивается вашим email).
   3. Перейдите на GitHub: Settings → SSH and GPG keys → New SSH key.
   4. Вставьте ключ в поле Key и дайте ему любое название (например, "WSL Laptop"). [2, 5, 6, 7, 8] 

## 3. Проверка соединения
Проверьте, что GitHub «узнает» вас:

ssh -T git@github.com

Вы должны увидеть: «Hi marychev! You've successfully authenticated...». [2, 9] 
## 4. Переключение репозитория на SSH
Ваш текущий репозиторий все еще настроен на HTTPS, поэтому он продолжает спрашивать пароль. Нужно сменить URL на SSH-версию:

git remote set-url origin git@github.com:marychev/gencrud_know_base.git

(Этот адрес можно найти на главной странице вашего репозитория под кнопкой Code → вкладка SSH). [10] 
## Почему это лучше?

* Безопасность: Приватный ключ никогда не передается по сети.
* Удобство: Больше не нужно вводить пароли или токены при каждом push или pull. [2, 11, 12] 
* 

Теперь попробуйте снова выполнить git push. Если вы устанавливали пароль на ключ (passphrase), система спросит его один раз.
Хотите узнать, как сделать так, чтобы не вводить пароль от ключа при каждом новом открытии терминала?

[1] [https://docs.github.com](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)
[2] [https://medium.com](https://medium.com/@kavyalegitimate/from-zero-to-hero-setting-up-ssh-with-github-in-wsl-a-beginners-guide-07a1dff3ce8a)
[3] [https://statistics.berkeley.edu](https://statistics.berkeley.edu/computing/ssh-keys#:~:text=You%20can%20generate%20keys%20with%20the%20%27ssh%2Dkeygen%27,Your%20identification%20has%20been%20saved%20in%20$HOME/.)
[4] [https://dev.to](https://dev.to/otumianempire/create-a-github-personal-access-token-and-ssh-for-your-github-repository-3741)
[5] [https://docs.github.com](https://docs.github.com/articles/adding-a-new-ssh-key-to-your-github-account)
[6] [https://habr.com](https://habr.com/ru/articles/755036/)
[7] [https://docs.github.com](https://docs.github.com/ru/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account)
[8] [https://www.ispsystem.ru](https://www.ispsystem.ru/docs/dcimanager-admin/nachalo-raboty/dobavlenie-ssh-klyuchej#:~:text=%D0%9F%D0%B5%D1%80%D0%B5%D0%B9%D0%B4%D0%B8%D1%82%D0%B5%20%D0%B2%20SSH%2D%D0%BA%D0%BB%D1%8E%D1%87%D0%B8%20%E2%86%92%20%D0%94%D0%BE%D0%B1%D0%B0%D0%B2%D0%B8%D1%82%D1%8C%20SSH%2D%D0%BA%D0%BB%D1%8E%D1%87.%20%D0%A3%D0%BA%D0%B0%D0%B6%D0%B8%D1%82%D0%B5,%D0%B8%20%D0%BE%D1%82%D0%BA%D1%80%D1%8B%D1%82%D1%83%D1%8E%20%D1%87%D0%B0%D1%81%D1%82%D1%8C%20%D0%A1%D0%BE%D0%B4%D0%B5%D1%80%D0%B6%D0%B8%D0%BC%D0%BE%D0%B3%D0%BE%20SSH%2D%D0%BA%D0%BB%D1%8E%D1%87%D0%B0.%20%D0%9D%D0%B0%D0%B6%D0%BC%D0%B8%D1%82%D0%B5%20%D0%94%D0%BE%D0%B1%D0%B0%D0%B2%D0%B8%D1%82%D1%8C.)
[9] [https://mgimond.github.io](https://mgimond.github.io/Colby-summer-git-workshop-2021/authenticating%20with%20github.html)
[10] [https://medium.com](https://medium.com/@ruthdawit312/git-authentication-methods-for-github-ssh-vs-https-5c94f718508f)
[11] [https://www.reddit.com](https://www.reddit.com/r/github/comments/18tskh9/is_there_any_good_reason_for_using_a_patpersonal/)
[12] [https://habr.com](https://habr.com/ru/articles/1006970/)
