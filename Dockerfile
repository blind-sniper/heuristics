# Catenae Link
# Copyright (C) 2018-2019 Rodrigo Martínez <dev@brunneis.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

FROM catenae/link:develop
RUN \
     pip install \
       numpy \
       scipy \
       scikit-learn==0.20.3
COPY *.py /opt/fuc-benchmark/
COPY binaries /opt/fuc-benchmark/binaries
COPY model_trainer /opt/fuc-benchmark/model_trainer
WORKDIR /opt/fuc-benchmark
