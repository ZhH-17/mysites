from django.urls import path
from calibration import views
from django.conf.urls import url

urlpatterns = [
    url(r'^calibration/listFullData$', views.list_full_data),
    url(r'^calibration/listSectionName$', views.list_section_name),
    url(r'^calibration/calculateRawData$', views.calculate_raw_data),
    url(r'^calibration/submitFullData$', views.submit_full_data),
    url(r'^calibration/getRawData$', views.get_raw_data),
    url(r'^calibration/insertRawData$', views.insert_raw_data),

    url(r'^calibration/software/listFullData$', views.list_full_data),
    url(r'^calibration/software/listSectionName$', views.list_section_name),
    url(r'^calibration/software/calculateRawData$', views.calculate_raw_data),
    url(r'^calibration/software/submitFullData$', views.submit_full_data),
]

