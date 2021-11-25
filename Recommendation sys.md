```yaml
title: recommendation_sys
author: Siming
date: 2021.11.12

```



# Recommendation sys



- v is the number of votes for the movie; 
- m is the minimum votes required to be listed in the chart;
- R is the average rating of the movie; And
- C is the mean vote across the whole report

```python
def weighted_rating(x, m=m, C=C):
    v = x['vote_count']
    R = x['vote_average']
    # Calculation based on the IMDB formula
    return (v/(v+m) * R) + (m/(m+v) * C)
```



## tf-idf vectorizer encode words to have movie similarity,.

```python
#Import TfIdfVectorizer from scikit-learn
from sklearn.feature_extraction.text import TfidfVectorizer

#Define a TF-IDF Vectorizer Object. Remove all english stop words such as 'the', 'a'
tfidf = TfidfVectorizer(stop_words='english')

#Replace NaN with an empty string
df2['overview'] = df2['overview'].fillna('')

#Construct the required TF-IDF matrix by fitting and transforming the data
tfidf_matrix = tfidf.fit_transform(df2['overview'])

#Output the shape of tfidf_matrix
tfidf_matrix.shape
```



![](C:\Users\dscshap3808\AppData\Roaming\Typora\typora-user-images\image-20211112110331570.png)

```python
# Import linear_kernel
from sklearn.metrics.pairwise import linear_kernel

# Compute the cosine similarity matrix
cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)

def get_recommendations(title, cosine_sim=cosine_sim):
    # Get the index of the movie that matches the title
    idx = indices[title]

    # Get the pairwsie similarity scores of all movies with that movie
    sim_scores = list(enumerate(cosine_sim[idx]))

    # Sort the movies based on the similarity scores
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)

    # Get the scores of the 10 most similar movies
    sim_scores = sim_scores[1:11]

    # Get the movie indices
    movie_indices = [i[0] for i in sim_scores]

    # Return the top 10 most similar movies
    return df2['title'].iloc[movie_indices]
```

The next steps are the same as what we did with our plot description based recommender. One important difference is that we use the **CountVectorizer()** instead of TF-IDF. This is because we do not want to down-weight the presence of an actor/director if he or she has acted or directed in relatively more movies. It doesn't make much intuitive sense.

```python
# Import CountVectorizer and create the count matrix
from sklearn.feature_extraction.text import CountVectorizer

count = CountVectorizer(stop_words='english')
count_matrix = count.fit_transform(df2['soup'])

# Compute the Cosine Similarity matrix based on the count_matrix
from sklearn.metrics.pairwise import cosine_similarity

cosine_sim2 = cosine_similarity(count_matrix, count_matrix)
# Reset index of our main DataFrame and construct reverse mapping as before
df2 = df2.reset_index()
indices = pd.Series(df2.index, index=df2['title'])
```



# **Collaborative Filtering**



1. **`User based filtering`**- These systems recommend products to a `user that similar users` have liked. For measuring the similarity between two users we can either use pearson correlation or cosine similarity. This filtering technique can be illustrated with an example. In the following matrixes, each row represents a user, while the columns correspond to different movies except the last one which records the similarity between that user and the target user. Each cell represents the rating that the user gives to that movie. Assume user E is the target.

![image-20211112111924100](C:\Users\dscshap3808\AppData\Roaming\Typora\typora-user-images\image-20211112111924100.png)



Since user A and F do not share any movie ratings in common with user E, their similarities with user E are not defined in **`Pearson` `Correlation`**. Therefore, we only need to consider user B, C, and D. Based on Pearson Correlation, we can compute the following similarity.



2. `**Item Based Collaborative Filtering**` 

![image-20211112112700661](C:\Users\dscshap3808\AppData\Roaming\Typora\typora-user-images\image-20211112112700661.png)



## Data

| user/item | 商品1 | 商品2 | 商品3 | 商品4 |
| --------- | ----- | ----- | ----- | ----- |
| 用户1     | 9     | -     | 6     | 8     |
| 用户2     | 3     | 9     | -     | 4     |
| 用户3     | -     | -     | 6     | 8     |
| 用户4     | 9     | 8     | 5     | 9     |
| 用户5     | 8     | 7     | 6     | -     |



| 商品特征\ 商品名 | 1    | 2    |
| ---------------- | ---- | ---- |
| K1               |      |      |
| k2               |      |      |
| k3               |      |      |



| user\ user喜好特征 | k1   | k2   | k3   |
| ------------------ | ---- | ---- | ---- |
|                    |      |      |      |
|                    |      |      |      |
|                    |      |      |      |

